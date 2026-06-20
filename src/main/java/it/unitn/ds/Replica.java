package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractReplica {

    // =========================================================================
    // Messages
    // =========================================================================

    // Client → replica: request to read positions[index].
    public static class ReadRequest implements Serializable {
        public final int index;
        public ReadRequest(int index) { this.index = index; }
    }

    // Replica → client: immediate reply carrying the current value of positions[index].
    public static class ReadResponse implements Serializable {
        public final int index;
        public final int value;
        public final int replicaId;
        public ReadResponse(int index, int value, int replicaId) {
            this.index = index; this.value = value; this.replicaId = replicaId;
        }
    }

    // Client → replica: request to write positions[index] = value. If the target is not the
    // coordinator, the replica forwards it and keeps a PendingForward entry for response routing.
    public static class WriteRequest implements Serializable {
        public final int index;
        public final int value;
        public WriteRequest(int index, int value) { this.index = index; this.value = value; }
    }

    // Replica → client: final acknowledgement that the write reached quorum and was committed.
    public static class WriteResponse implements Serializable {
        public final boolean success;
        public final int index;
        public final int value;
        public final int replicaId;
        public WriteResponse(boolean success, int index, int value, int replicaId) {
            this.success = success; this.index = index; this.value = value; this.replicaId = replicaId;
        }
    }

    // Relay sent by a non-coordinator to the coordinator; carries the original client ref so
    // the coordinator can route the WriteResponse back without knowing the client directly.
    public static class ForwardWrite implements Serializable {
        public final int index;
        public final int value;
        public final ActorRef originalClient;
        public ForwardWrite(int index, int value, ActorRef originalClient) {
            this.index = index; this.value = value; this.originalClient = originalClient;
        }
    }

    // Coordinator → all replicas (2PC phase 1): proposes a write identified by (epoch, seqNum).
    // originReplica and originClient are threaded through so the commit path can send WriteResponse
    // back to the right client without extra state in each replica.
    public static class Update implements Serializable {
        public final int epoch;
        public final int seqNum;
        public final int index;
        public final int value;
        public final ActorRef originReplica;
        public final ActorRef originClient;
        public Update(int epoch, int seqNum, int index, int value,
                      ActorRef originReplica, ActorRef originClient) {
            this.epoch = epoch; this.seqNum = seqNum;
            this.index = index; this.value = value;
            this.originReplica = originReplica; this.originClient = originClient;
        }
    }

    // Replica → coordinator (2PC phase 1 reply): signals that the replica stored the UPDATE
    // and is ready to commit; counted by the coordinator towards the quorum.
    public static class Ack implements Serializable {
        public final int epoch;
        public final int seqNum;
        public Ack(int epoch, int seqNum) { this.epoch = epoch; this.seqNum = seqNum; }
    }

    // Coordinator → all replicas (2PC phase 2): commit order for the update identified by
    // (epoch, seqNum); on receipt each replica writes to positions[] and fires the callback.
    public static class WriteOk implements Serializable {
        public final int epoch;
        public final int seqNum;
        public WriteOk(int epoch, int seqNum) { this.epoch = epoch; this.seqNum = seqNum; }
    }

    // Coordinator → all replicas: periodic liveness signal; receiving one resets the replica's
    // heartbeat timeout, silence for 2× the interval is treated as a coordinator crash.
    public static class Heartbeat implements Serializable {}
    // Self-message sent by the scheduler to trigger the next periodic heartbeat broadcast.
    private static class BroadcastHeartbeat implements Serializable {}
    // Self-message fired when no heartbeat has arrived within the deadline; initiates election.
    private static class HeartbeatTimeout implements Serializable {}

    // One replica's vote record in the ELECTION ring message: the last (epoch, seqNum) it committed.
    public static class ElectionEntry implements Serializable {
        public final int replicaId;
        public final int epoch;
        public final int seqNum;
        public ElectionEntry(int replicaId, int epoch, int seqNum) {
            this.replicaId = replicaId; this.epoch = epoch; this.seqNum = seqNum;
        }
    }

    // Travels around the ring accumulating one ElectionEntry per live replica;
    // when it returns to the initiator all live replicas are represented.
    public static class Election implements Serializable {
        public final List<ElectionEntry> entries;
        public final int crashedCoordId;
        public Election(List<ElectionEntry> entries, int crashedCoordId) {
            this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
            this.crashedCoordId = crashedCoordId;
        }
    }

    // Ring-hop target → sender: immediate receipt acknowledgement for an ELECTION message.
    // Without it the sender cannot distinguish a slow hop from a crashed one.
    // replicaId identifies the responder so a late ACK from a hop already skipped
    // does not cancel the timeout armed for the current target.
    public static class ElectionAck implements Serializable {
        public final int replicaId;
        public ElectionAck(int replicaId) { this.replicaId = replicaId; }
    }

    // Self-message: fired when the current ring-hop target did not ACK in time; skip and retry.
    private static class ElectionAckTimeout implements Serializable {
        public final int targetId;
        public ElectionAckTimeout(int targetId) { this.targetId = targetId; }
    }

    // New coordinator → all replicas: announces leadership and delivers the full committed history
    // so lagging replicas can apply any updates they missed before the election.
    public static class Synchronization implements Serializable {
        public final int newCoordId;
        public final int newEpoch;
        public final List<Update> history;
        public Synchronization(int newCoordId, int newEpoch, List<Update> history) {
            this.newCoordId = newCoordId; this.newEpoch = newEpoch;
            this.history = Collections.unmodifiableList(new ArrayList<>(history));
        }
    }

    // =========================================================================
    // State
    // =========================================================================

    // Holds a write received from a client while this replica was not the coordinator.
    // Kept until the update is committed (for response routing) and used to re-forward after election.
    private static class PendingForward {
        final int index;
        final int value;
        final ActorRef client;
        PendingForward(int index, int value, ActorRef client) {
            this.index = index; this.value = value; this.client = client;
        }
    }

    private final int[] positions = new int[POSITIONS_LIST_LENGTH];
    private Map<Integer, ActorRef> group = Collections.emptyMap();
    private int coordinatorId;
    private int currentEpoch = 0;
    private int sequenceNumber = 0;
    private final List<Update> updateHistory = new ArrayList<>();
    // Coordinator: updates awaiting quorum ACKs. Non-coordinator: updates awaiting WriteOk.
    private final Map<Long, Update> pendingUpdates = new HashMap<>();
    // Coordinator only: tracks which replica IDs have ACKed each in-flight update.
    private final Map<Long, Set<Integer>> pendingAcks = new HashMap<>();
    private final List<PendingForward> pendingForwards = new ArrayList<>();

    // trackers for crash state, crash type, and number of messages before crash is started 
    private boolean crashed = false;
    private Crash.Type pendingCrashType = null;
    private int pendingCrashCount = 0;

    private Cancellable heartbeatSchedule;
    private Cancellable heartbeatTimeoutSchedule;
    private Cancellable electionAckTimeoutSchedule;

    // Guards against starting a second election while one is already in progress.
    private boolean inElection = false;
    private int electionCrashedCoordId = -1; // TODO: right now it's not being used, decide if it's needed for crashed election edge cases, otherwise remove
    private Election currentElection;
    // Crashed coordinator plus any ring-hop targets that timed out in the current election round.
    private final Set<Integer> electionSkipped = new HashSet<>();
    // tracks the last replica that was forwarded an election message (not necessarily the successor due to crashes)
    private int electionCurrentTarget = -1; 

    // =========================================================================
    // Constructors / Props
    // =========================================================================

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
             AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency,
                   int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    public static Props propsWithListener(int id, int minLatency, int maxLatency,
                                          int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    // =========================================================================
    // Abstract methods
    // =========================================================================

    @Override
    public int getSystemNumberOfActors() { return group.size(); }

    @Override
    public void initSystem(InitSystem sysInit) {
        this.group = sysInit.group;
        this.coordinatorId = sysInit.coordinator_id;
        if (id == coordinatorId) {
            sendHeartbeats();
            scheduleNextHeartbeat();
        } else {
            scheduleHeartbeatTimeout();
        }
    }

    @Override
    public void crash(Crash how) {
        if (how.type == Crash.Type.Now) {
            applyCrashNow();
        } else {
            pendingCrashType = how.type;
            pendingCrashCount = how.after_n_messages_of_type;
            // after_n=0 means "crash immediately on next message of that type"; handle it like Now
            if (pendingCrashCount <= 0) {
                pendingCrashType = null;
                applyCrashNow();
            }
        }
    }

    // =========================================================================
    // Receive
    // =========================================================================

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadRequest.class, this::onReadRequest)
                .match(WriteRequest.class, this::onWriteRequest)
                .match(ForwardWrite.class, this::onForwardWrite)
                .match(Update.class, this::onUpdate)
                .match(Ack.class, this::onAck)
                .match(WriteOk.class, this::onWriteOk)
                .match(Heartbeat.class, this::onHeartbeat)
                .match(BroadcastHeartbeat.class, this::onBroadcastHeartbeat)
                .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
                .match(Election.class, this::onElection)
                .match(ElectionAck.class, this::onElectionAck)
                .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
                .match(Synchronization.class, this::onSynchronization)
                .build();
    }

    // =========================================================================
    // Read / Write handlers
    // =========================================================================

    // reads are immediate, returning the value of the local replica
    private void onReadRequest(ReadRequest msg) {
        if (crashed) return;
        tell(new ReadResponse(msg.index, positions[msg.index], id), getSender());
    }

    // writes are forwarded to the coordinator
    private void onWriteRequest(WriteRequest msg) {
        if (crashed) return;
        if (id == coordinatorId) {
            coordinatorAcceptWrite(msg.index, msg.value, getSelf(), getSender());
        } else {
            pendingForwards.add(new PendingForward(msg.index, msg.value, getSender()));
            tell(new ForwardWrite(msg.index, msg.value, getSender()), group.get(coordinatorId));
        }
    }

    private void onForwardWrite(ForwardWrite msg) {
        if (crashed) return;
        if (id != coordinatorId) return; // stale delivery after a leadership change; discard TODO: make sure this does not drop client write requests, if not explain it in comment here for completeness
        coordinatorAcceptWrite(msg.index, msg.value, getSender(), msg.originalClient);
    }

    // coordinator serializes a write request, broadcasts the update
    private void coordinatorAcceptWrite(int index, int value, ActorRef originReplica, ActorRef originClient) {
        int seqNum = sequenceNumber++;
        Update update = new Update(currentEpoch, seqNum, index, value, originReplica, originClient);
        long k = key(currentEpoch, seqNum);
        pendingUpdates.put(k, update);
        Set<Integer> acks = new HashSet<>();
        acks.add(id); // coordinator self-ACKs; also covers the N=1 single-replica case
        pendingAcks.put(k, acks);
        broadcastToOthers(update);
        // if quorum already met (single replica): commit immediately without waiting for external ACKs (otherwise we would be waiting forever for another ACK to confirm write success)
        if (acks.size() >= quorum()) {
            broadcastWriteOk(update);
            pendingAcks.remove(k);
        }
    }

    private void onUpdate(Update msg) {
        if (checkAndApplyCrash(Crash.Type.Update)) return;
        long k = key(msg.epoch, msg.seqNum);
        if (!pendingUpdates.containsKey(k)) {
            pendingUpdates.put(k, msg); // store only once; duplicate UPDATE must not overwrite
        }
        tell(new Ack(msg.epoch, msg.seqNum), getSender());
    }

    private void onAck(Ack msg) {
        if (crashed) return;
        if (id != coordinatorId) return;
        long k = key(msg.epoch, msg.seqNum);
        Set<Integer> acks = pendingAcks.get(k);
        if (acks == null) return;
        int senderId = senderIdOf(getSender());
        if (senderId < 0) return;
        acks.add(senderId);
        if (acks.size() >= quorum()) {
            Update u = pendingUpdates.get(k);
            if (u != null) {
                broadcastWriteOk(u);
            }
            pendingAcks.remove(k); // remove before any late ACKs can re-trigger WriteOk for the same update
        }
    }

    private void broadcastWriteOk(Update update) {
        broadcastToOthers(new WriteOk(update.epoch, update.seqNum));
        applyUpdate(update); // coordinator commits to itself as well (it does not send itself a WriteOk)
    }

    private void onWriteOk(WriteOk msg) {
        if (checkAndApplyCrash(Crash.Type.WriteOK)) return;
        long k = key(msg.epoch, msg.seqNum);
        Update u = pendingUpdates.get(k);
        if (u == null) return;
        if (historyContains(u.epoch, u.seqNum)) return; // already committed via SYNCHRONIZATION; skip
        applyUpdate(u);
    }

    private void applyUpdate(Update u) {
        positions[u.index] = u.value;
        updateHistory.add(u);
        pendingUpdates.remove(key(u.epoch, u.seqNum));
        log("applied update " + u.epoch + ":" + u.seqNum + " (" + u.index + ", " + u.value + ")");
        callbackOnUpdateApplied(u.index, u.value);
        if (u.originReplica.equals(getSelf())) {
            tell(new WriteResponse(true, u.index, u.value, id), u.originClient);
            // remove the pending forward that triggered this update (match by content, first occurrence)
            for (int i = 0; i < pendingForwards.size(); i++) {
                PendingForward pf = pendingForwards.get(i);
                if (pf.index == u.index && pf.value == u.value && pf.client.equals(u.originClient)) {
                    pendingForwards.remove(i);
                    break;
                }
            }
        }
    }

    // =========================================================================
    // Heartbeat
    // =========================================================================

    private void sendHeartbeats() {
        for (Map.Entry<Integer, ActorRef> e : group.entrySet()) {
            if (e.getKey() != id) tell(new Heartbeat(), e.getValue());
        }
    }

    private void scheduleNextHeartbeat() {
        cancel(heartbeatSchedule);
        heartbeatSchedule = getContext().system().scheduler().scheduleOnce(
                Duration.create(getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                getSelf(),
                new BroadcastHeartbeat(),
                getContext().system().dispatcher(),
                getSelf());
    }

    private void onBroadcastHeartbeat(BroadcastHeartbeat msg) {
        if (crashed) return;
        if (id != coordinatorId) return;
        sendHeartbeats();
        scheduleNextHeartbeat();
    }

    private void scheduleHeartbeatTimeout() {
        cancel(heartbeatTimeoutSchedule);
        // 2× beat interval: one full interval for the coordinator to send + one for max network latency
        heartbeatTimeoutSchedule = getContext().system().scheduler().scheduleOnce(
                Duration.create(getCoordinatorBeatInterval() * 2L, TimeUnit.MILLISECONDS),
                getSelf(),
                new HeartbeatTimeout(),
                getContext().system().dispatcher(),
                getSelf());
    }

    private void onHeartbeat(Heartbeat msg) {
        if (checkAndApplyCrash(Crash.Type.Heartbeat)) return;
        if (id == coordinatorId) return;
        scheduleHeartbeatTimeout();
    }

    private void onHeartbeatTimeout(HeartbeatTimeout msg) {
        if (crashed) return;
        if (inElection) return;
        if (id == coordinatorId) return;
        startElection(coordinatorId);
    }

    // =========================================================================
    // Election (ring-based)
    // =========================================================================

    private void startElection(int crashedCoordId) {
        inElection = true;
        electionCrashedCoordId = crashedCoordId;
        electionSkipped.clear();
        electionSkipped.add(crashedCoordId);
        callbackOnElectionStarted(crashedCoordId);

        List<ElectionEntry> entries = new ArrayList<>();
        entries.add(new ElectionEntry(id, lastEpoch(), lastSeqNum()));
        currentElection = new Election(entries, crashedCoordId);
        forwardElection();
    }

    private void forwardElection() {
        int target = nextRingTarget();
        if (target == -1) {
            log("Election: no available target");
            return;
        }
        electionCurrentTarget = target;
        tell(currentElection, group.get(target));
        cancel(electionAckTimeoutSchedule);
        electionAckTimeoutSchedule = getContext().system().scheduler().scheduleOnce(
                Duration.create(getMaxLatencyPlusTolerance() * 3L, TimeUnit.MILLISECONDS),
                getSelf(),
                new ElectionAckTimeout(target),
                getContext().system().dispatcher(),
                getSelf());
    }

    // Walk the ring clockwise (by sorted ID) skipping the crashed coordinator and any timed-out nodes
    private int nextRingTarget() {
        List<Integer> ids = new ArrayList<>(group.keySet());
        Collections.sort(ids);
        int n = ids.size();
        int selfIdx = ids.indexOf(id);
        for (int step = 1; step <= n; step++) {
            int cand = ids.get((selfIdx + step) % n);
            if (cand == id) continue;
            if (electionSkipped.contains(cand)) continue;
            return cand;
        }
        return -1;
    }

    private void onElection(Election msg) {
        if (checkAndApplyCrash(Crash.Type.Election)) return;
        tell(new ElectionAck(id), getSender());

        boolean alreadyInRing = false;
        for (ElectionEntry e : msg.entries) {
            if (e.replicaId == id) { alreadyInRing = true; break; }
        }

        if (alreadyInRing) {
            // message has completed the ring; entries contain every live replica — elect the best one
            ElectionEntry winner = computeWinner(msg.entries);
            if (winner.replicaId == id) {
                becomeCoordinator();
            } else {
                tell(msg, group.get(winner.replicaId)); // let the winner know it won
            }
        } else {
            if (!inElection) {
                inElection = true;
                electionCrashedCoordId = msg.crashedCoordId;
                electionSkipped.clear();
                electionSkipped.add(msg.crashedCoordId);
                callbackOnElectionStarted(msg.crashedCoordId);
            }
            List<ElectionEntry> newEntries = new ArrayList<>(msg.entries);
            newEntries.add(new ElectionEntry(id, lastEpoch(), lastSeqNum()));
            currentElection = new Election(newEntries, msg.crashedCoordId);
            forwardElection();
        }
    }

    // Priority: highest epoch → highest seqNum → highest id (tiebreaker)
    private ElectionEntry computeWinner(List<ElectionEntry> entries) {
        ElectionEntry best = entries.get(0);
        for (ElectionEntry e : entries) {
            if (e.epoch > best.epoch
                    || (e.epoch == best.epoch && e.seqNum > best.seqNum)
                    || (e.epoch == best.epoch && e.seqNum == best.seqNum && e.replicaId > best.replicaId)) {
                best = e;
            }
        }
        return best;
    }

    private void onElectionAck(ElectionAck msg) {
        if (crashed) return;
        // ignore late ACKs from hops already skipped: they would otherwise cancel
        // the timeout armed for the new (current) target and stall the election
        if (msg.replicaId != electionCurrentTarget) return;
        cancel(electionAckTimeoutSchedule);
    }

    private void onElectionAckTimeout(ElectionAckTimeout msg) {
        if (crashed) return;
        if (msg.targetId != electionCurrentTarget) return;
        electionSkipped.add(msg.targetId);
        forwardElection();
    }

    private void becomeCoordinator() {
        if (id == coordinatorId && !inElection) return;
        coordinatorId = id;
        currentEpoch = Math.max(currentEpoch, lastEpoch()) + 1; // new epoch ensures updates are distinguishable from pre-crash ones
        sequenceNumber = 0;
        inElection = false;
        cancel(heartbeatTimeoutSchedule);
        cancel(electionAckTimeoutSchedule);
        callbackOnCoordinatorElected(id);
        broadcastToOthers(new Synchronization(id, currentEpoch, updateHistory));
        sendHeartbeats();
        scheduleNextHeartbeat();
        // replay writes that arrived during the election so they are not lost
        for (PendingForward pf : new ArrayList<>(pendingForwards)) {
            coordinatorAcceptWrite(pf.index, pf.value, getSelf(), pf.client);
        }
    }

    private void onSynchronization(Synchronization msg) {
        if (crashed) return;
        coordinatorId = msg.newCoordId;
        currentEpoch = msg.newEpoch;
        sequenceNumber = 0;
        inElection = false;
        cancel(electionAckTimeoutSchedule);
        // Catch-up: route every missed update through applyUpdate so that
        //  - WriteResponse is sent to the client if this replica was the origin
        //    (otherwise the client would time out for an actually-committed write)
        //  - the matching pendingForward is removed (otherwise the next loop would
        //    re-forward an already-committed write and trigger a duplicate apply)
        for (Update u : msg.history) {
            if (!historyContains(u.epoch, u.seqNum)) {
                applyUpdate(u);
            }
        }
        scheduleHeartbeatTimeout();
        callbackOnCoordinatorElected(msg.newCoordId);
        // re-forward only the writes still pending after catch-up; iterate over a snapshot
        // since applyUpdate may have pruned the list above
        for (PendingForward pf : new ArrayList<>(pendingForwards)) {
            tell(new ForwardWrite(pf.index, pf.value, pf.client), group.get(msg.newCoordId));
        }
    }

    // =========================================================================
    // Crash handling
    // =========================================================================

    private void applyCrashNow() {
        crashed = true;
        cancel(heartbeatSchedule);
        cancel(heartbeatTimeoutSchedule);
        cancel(electionAckTimeoutSchedule);
    }

    // Called at the top of every handler that has a crash point.
    // Returns true (and crashes) BEFORE the message is processed, so the Nth message is never handled.
    private boolean checkAndApplyCrash(Crash.Type type) {
        if (crashed) return true;
        if (pendingCrashType == type) {
            if (pendingCrashCount <= 0) {
                pendingCrashType = null;
                applyCrashNow();
                return true;
            }
            pendingCrashCount--;
        }
        return false;
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private void cancel(Cancellable c) {
        if (c != null && !c.isCancelled()) c.cancel();
    }

    private int quorum() { return group.size() / 2 + 1; }

    // Packs (epoch, seqNum) into a single long for use as a map key
    private long key(int epoch, int seqNum) {
        return ((long) epoch << 32) | (seqNum & 0xFFFFFFFFL);
    }

    // As suggested in README, broadcast uses the tell function to unify crash handling
    private void broadcastToOthers(Serializable msg) {
        for (Map.Entry<Integer, ActorRef> e : group.entrySet()) {
            if (e.getKey() != id) tell(msg, e.getValue());
        }
    }

    private Update lastUpdate() {
        return updateHistory.isEmpty() ? null : updateHistory.get(updateHistory.size() - 1);
    }

    private int lastEpoch() { Update u = lastUpdate(); return u == null ? -1 : u.epoch; }
    private int lastSeqNum() { Update u = lastUpdate(); return u == null ? -1 : u.seqNum; }

    private boolean historyContains(int epoch, int seqNum) {
        for (Update u : updateHistory) {
            if (u.epoch == epoch && u.seqNum == seqNum) return true;
        }
        return false;
    }

    private int senderIdOf(ActorRef ref) {
        for (Map.Entry<Integer, ActorRef> e : group.entrySet()) {
            if (e.getValue().equals(ref)) return e.getKey();
        }
        return -1;
    }
}
