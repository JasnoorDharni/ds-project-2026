package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
    public static class UpdateAck implements Serializable {
        public final int epoch;
        public final int seqNum;
        public UpdateAck(int epoch, int seqNum) { this.epoch = epoch; this.seqNum = seqNum; }
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
    // Messages > Self-scheduling
    // =========================================================================

    // fired when the current ring-hop target did not ACK in time; skip and retry.
    private static class ElectionAckTimeout implements Serializable {
        public final int targetId;
        public ElectionAckTimeout(int targetId) { this.targetId = targetId; }
    }

    // fired when an election has been running too long without producing a
    // SYNCHRONIZATION; covers the case where the elected winner crashes before broadcasting SYNC.
    private static class ElectionTerminationTimeout implements Serializable {}

    // fired if UPDATE not received after sending ForwardWrite
    private static class ForwardWriteTimeoutMsg implements Serializable {
        public final long forwardId;
        public ForwardWriteTimeoutMsg(long forwardId) { this.forwardId = forwardId; }
    }

    // fired if WRITEOK not received in time after sending Ack
    private static class PendingWriteOkTimeoutMsg implements Serializable {
        public final int epoch;
        public final int seqNum;
        public PendingWriteOkTimeoutMsg(int epoch, int seqNum) { this.epoch = epoch; this.seqNum = seqNum; }
    }

    // fired after delay from staggeredElectionStartSchedule
    public static class StaggeredElectionStartTimeout {
        public final int crashedCoordId;
        public StaggeredElectionStartTimeout(int crashedCoordId) {
            this.crashedCoordId = crashedCoordId;
        }
    }

    // =========================================================================
    // State
    // =========================================================================

    // Holds a write received from a client while this replica was not the coordinator.
    // Kept until the update is committed (for response routing) and used to re-forward after election.
    private static class PendingForward {
        final long id;       // used to identify the timer
        final int index;
        final int value;
        final ActorRef client;
        PendingForward(long id, int index, int value, ActorRef client) {
            this.id = id; this.index = index; this.value = value; this.client = client;
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
    private Crash.Type pendingCrashType = null;
    private int pendingCrashCount = 0;

    private Cancellable heartbeatSchedule;
    private Cancellable heartbeatTimeoutSchedule;
    private Cancellable electionAckTimeoutSchedule;
    private Cancellable electionTerminationTimeoutSchedule;
    private Cancellable staggeredElectionStartSchedule;

    private long nextForwardId = 0;
    private final Map<Long, Cancellable> forwardTimers = new HashMap<>();
    private final Map<Long, Cancellable> updateTimers = new HashMap<>();

    // Guards against starting a second election while one is already in progress.
    private boolean inElection = false;
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

    // this is handleCrashRequest basically
    @Override
    public void crash(Crash how) {
        if ((how.type == Crash.Type.Now) || (pendingCrashCount <= 0) ){
            applyCrashNow();
        } else {
            pendingCrashType = how.type;
            pendingCrashCount = how.after_n_messages_of_type;
        }
    }

    // =========================================================================
    // Receivers
    // =========================================================================

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadRequest.class, this::onReadRequest)
                .match(WriteRequest.class, this::onWriteRequest)
                .match(ForwardWrite.class, this::onForwardWrite)
                .match(Update.class, this::onUpdate)
                .match(UpdateAck.class, this::onUpdateAck)
                .match(WriteOk.class, this::onWriteOk)
                .match(Heartbeat.class, this::onHeartbeat)
                .match(BroadcastHeartbeat.class, this::onBroadcastHeartbeat)
                .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
                .match(Election.class, this::onElection)
                .match(ElectionAck.class, this::onElectionAck)
                .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
                .match(ElectionTerminationTimeout.class, this::onElectionTerminationTimeout)
                .match(Synchronization.class, this::onSynchronization)
                .match(ForwardWriteTimeoutMsg.class, this::onForwardWriteTimeout)
                .match(PendingWriteOkTimeoutMsg.class, this::onPendingWriteOkTimeout)
                .match(StaggeredElectionStartTimeout.class, this::onStaggeredElectionStartTimeout)
                .build();
    }

    private Receive electionReceive() {
        return createBaseReceiveBuilder()
                .match(ReadRequest.class, this::onReadRequest)
                .match(WriteRequest.class, this::onWriteRequest)
                .match(ForwardWrite.class, this::onForwardWrite)
                .match(Update.class, this::onUpdate)
                .match(UpdateAck.class, this::onUpdateAck)
                .match(WriteOk.class, this::onWriteOk)
                .match(Heartbeat.class, x -> {})
                .match(BroadcastHeartbeat.class, x -> {})
                .match(HeartbeatTimeout.class, x -> {} )
                .match(Election.class, this::onElection)
                .match(ElectionAck.class, this::onElectionAck)
                .match(ElectionAckTimeout.class, this::onElectionAckTimeout)
                .match(ElectionTerminationTimeout.class, this::onElectionTerminationTimeout)
                .match(Synchronization.class, this::onSynchronization)
                .match(ForwardWriteTimeoutMsg.class, x -> {})
                .match(PendingWriteOkTimeoutMsg.class, x -> {})
                .match(StaggeredElectionStartTimeout.class, this::onStaggeredElectionStartTimeout)
                .build();
    }

    private Receive crashedReceive() {
    return receiveBuilder()
            .matchAny(msg -> { })
            .build();
    }

    // =========================================================================
    // Read / Write handlers
    // =========================================================================

    // reads are immediate, returning the value of the local replica
    private void onReadRequest(ReadRequest msg) {
        tell(new ReadResponse(msg.index, positions[msg.index], id), getSender());
    }

    // writes are forwarded to the coordinator
    private void onWriteRequest(WriteRequest msg) {
        if (id == coordinatorId) {
            coordinatorAcceptWrite(msg.index, msg.value, getSelf(), getSender());
        } else {
            log("forwarding write request to "+coordinatorId);
            long fId = nextForwardId++;
            pendingForwards.add(new PendingForward(fId, msg.index, msg.value, getSender()));
            tell(new ForwardWrite(msg.index, msg.value, getSender()), group.get(coordinatorId));
            armForwardTimer(fId);
        }
    }

    private void onForwardWrite(ForwardWrite msg) {
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
        log("starting to listen for UpdateAcks, acks: "+ acks.size()+" quorum: "+ quorum());
        if (acks.size() >= quorum()) {
            broadcastWriteOk(update);
            pendingAcks.remove(k);
        }
    }

    private void onUpdate(Update msg) {
        long k = key(msg.epoch, msg.seqNum);
        if (!pendingUpdates.containsKey(k)) {
            pendingUpdates.put(k, msg);
        }

        if (id != coordinatorId) {
            cancel(updateTimers.get(k));
            Cancellable c = getContext().system().scheduler().scheduleOnce(
                    Duration.create(getMaxLatencyPlusTolerance() * 4L, TimeUnit.MILLISECONDS),
                    getSelf(),
                    new PendingWriteOkTimeoutMsg(msg.epoch, msg.seqNum),
                    getContext().system().dispatcher(),
                    getSelf());
            updateTimers.put(k, c);
        }

        if (msg.originReplica.equals(getSelf())) {
            for (PendingForward pf : pendingForwards) {
                if (pf.index == msg.index && pf.value == msg.value && pf.client.equals(msg.originClient)) {
                    cancel(forwardTimers.remove(pf.id));
                    break;
                }
            }
        }

        tell(new UpdateAck(msg.epoch, msg.seqNum), getSender());
        checkAndApplyCrash(Crash.Type.Update);
    }

    private void onUpdateAck(UpdateAck msg) {
        if (id != coordinatorId) return;
        long k = key(msg.epoch, msg.seqNum);
        Set<Integer> acks = pendingAcks.get(k);
        if (acks == null) return;
        int senderId = senderIdOf(getSender());
        if (senderId < 0) return;
        acks.add(senderId);
        log("received UpdateAck, acks:"+ acks.size()+" quorum: "+ quorum());
        if (acks.size() >= quorum()) {
            Update u = pendingUpdates.get(k);
            if (u != null) {
                broadcastWriteOk(u);
            }
            pendingAcks.remove(k); // remove before any late ACKs can re-trigger WriteOk for the same update
        }
    }

    private void broadcastWriteOk(Update update) {
        log("broadcasting WriteOk");
        broadcastToOthers(new WriteOk(update.epoch, update.seqNum));
        applyUpdate(update); // coordinator commits to itself as well (it does not send itself a WriteOk)
    }

    private void onWriteOk(WriteOk msg) {
        long k = key(msg.epoch, msg.seqNum);
        Update u = pendingUpdates.get(k);
        if (u == null) return;
        if (historyContains(u.epoch, u.seqNum)) return; // already committed via SYNCHRONIZATION; skip
        applyUpdate(u);
    }

    private void applyUpdate(Update u) {
        cancel(updateTimers.remove(key(u.epoch, u.seqNum)));
        positions[u.index] = u.value;
        updateHistory.add(u);
        pendingUpdates.remove(key(u.epoch, u.seqNum));
        log("applied update " + u.epoch + ":" + u.seqNum + " (" + u.index + ", " + u.value + ")");
        callbackOnUpdateApplied(u.index, u.value);
        if (u.originReplica.equals(getSelf())) {
            tell(new WriteResponse(true, u.index, u.value, id), u.originClient);
            for (int i = 0; i < pendingForwards.size(); i++) {
                PendingForward pf = pendingForwards.get(i);
                if (pf.index == u.index && pf.value == u.value && pf.client.equals(u.originClient)) {
                    cancel(forwardTimers.remove(pf.id));
                    pendingForwards.remove(i);
                    break;
                }
            }
        }
    }

    // =========================================================================
    // Heartbeat > Coordinator
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
        if (id != coordinatorId) return;
        sendHeartbeats();
        scheduleNextHeartbeat();
    }

    // =========================================================================
    // Heartbeat > Other Replicas
    // =========================================================================

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
        if (id == coordinatorId) return;
        scheduleHeartbeatTimeout();
        checkAndApplyCrash(Crash.Type.Heartbeat);
    }

    private void onHeartbeatTimeout(HeartbeatTimeout msg) {
        if (id == coordinatorId) return;
        log("Heartbeat timeout. Assuming coordinator " + coordinatorId + " crashed.");
        armStaggeredElectionStartTimeout(coordinatorId);
    }


    // =========================================================================
    // Other Timeouts
    // =========================================================================

    // this and the next timeout instantly start election instead of using the staggered approach,
    // as this timeout happens to individual replicas and not all at once like with the heartbeat
    private void onForwardWriteTimeout(ForwardWriteTimeoutMsg msg) {
        if (forwardTimers.remove(msg.forwardId) != null) {
            log("Timeout waiting for UPDATE after ForwardWrite. Assuming coordinator crash.");
            startElection(coordinatorId);
        }
    }

    private void onPendingWriteOkTimeout(PendingWriteOkTimeoutMsg msg) {
        long k = key(msg.epoch, msg.seqNum);
        if (updateTimers.remove(k) != null) {
            log("Timeout waiting for WRITEOK. Assuming coordinator crash.");
            startElection(coordinatorId);
        }
    }


    // =========================================================================
    // Election > Staggered Start 
    // =========================================================================

    // replicas that notice coordinator crash through heartbeats will pass delay = id*getMaxLatencyPlusTolerance, replicas that notice through write attemps will pass 0
    private void armStaggeredElectionStartTimeout(int crashedCoordId){
        cancel(staggeredElectionStartSchedule);
        long delay = id * getMaxLatencyPlusTolerance();
        staggeredElectionStartSchedule = getContext().system().scheduler().scheduleOnce(
                Duration.create(delay, TimeUnit.MILLISECONDS),
                getSelf(),
                new StaggeredElectionStartTimeout(crashedCoordId),
                getContext().system().dispatcher(),
                getSelf());
    }


    private void onStaggeredElectionStartTimeout(StaggeredElectionStartTimeout msg) {
        log("Starting election after random timeout...");
        startElection(msg.crashedCoordId);
    }


    private void startElection(int crashedCoordId) {
        inElection = true;
        electionSkipped.clear();
        electionSkipped.add(crashedCoordId);
        callbackOnElectionStarted(crashedCoordId);
        armElectionTerminationTimeout();

        List<ElectionEntry> entries = new ArrayList<>();
        entries.add(new ElectionEntry(id, lastEpoch(), lastSeqNum()));
        currentElection = new Election(entries, crashedCoordId);
        forwardElection();
    }


    // =========================================================================
    // Election > ring forwarding
    // =========================================================================


    private void forwardElection() {
        int target = nextRingTarget();
        if (target == -1) {
            log("Election: no available target");
            return;
        }
        electionCurrentTarget = target;
        tell(currentElection, group.get(target));
        log("forwarded Election message to #"+electionCurrentTarget);
        armElectionAckTimeout(target);
    }

    // Walk the ring clockwise (by sorted ID) skipping the crashed coordinator and any timed-out nodes
    private int nextRingTarget() {
        List<Integer> ids = new ArrayList<>(group.keySet());
        Collections.sort(ids);
        int n = ids.size();
        int selfIdx = ids.indexOf(id);
        for (int step = 1; step <= n; step++) {
            int cand = ids.get((selfIdx + step) % n);
            // if (cand == id) continue;
            if (electionSkipped.contains(cand)) continue;
            return cand;
        }
        return -1;
    }

    private void onElection(Election msg) {
        // avoid starting another election while still forwarding the current one
        cancel(staggeredElectionStartSchedule);

        tell(new ElectionAck(id), getSender());

        boolean alreadyInRing = false;
        for (ElectionEntry e : msg.entries) {
            if (e.replicaId == id) { alreadyInRing = true; break; }
        }

        if (alreadyInRing) {
            // message has completed the ring; entries contain every live replica — elect the best one
            ElectionEntry winner = computeWinner(msg.entries);
            if (winner.replicaId == id) {
                log("message completed ring, this replica is the winner");
                becomeCoordinator();
            } else {
                log("message completed ring, notifying winner #"+winner.replicaId);
                tell(msg, group.get(winner.replicaId)); // let the winner know it won
            }
        } else {
            if (!inElection) {
                inElection = true;
                electionSkipped.clear();
                electionSkipped.add(msg.crashedCoordId);
                callbackOnElectionStarted(msg.crashedCoordId);
                armElectionTerminationTimeout();
            }
            List<ElectionEntry> newEntries = new ArrayList<>(msg.entries);
            newEntries.add(new ElectionEntry(id, lastEpoch(), lastSeqNum()));
            currentElection = new Election(newEntries, msg.crashedCoordId);
            forwardElection();
        }
        checkAndApplyCrash(Crash.Type.Election);
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
        // ignore late ACKs from hops already skipped: they would otherwise cancel
        // the timeout armed for the new (current) target and stall the election
        if (msg.replicaId != electionCurrentTarget) return;
        cancel(electionAckTimeoutSchedule);
    }

    // =========================================================================
    // Election > timeout handlers
    // =========================================================================


    private void onElectionAckTimeout(ElectionAckTimeout msg) {
        if (msg.targetId != electionCurrentTarget) return;
        debug("Election ACK timeout for target " + msg.targetId + ". Skipping.");
        electionSkipped.add(msg.targetId); // should we reset the nodes that are member of the election so that they know to redo the loop or not needed?
        forwardElection();
    }

    private void onElectionTerminationTimeout(ElectionTerminationTimeout msg) {
        if (!inElection) return; // already resolved via SYNCHRONIZATION

        // Election stalled (winner likely crashed before sending SYNC); restart.
        log("Election termination timeout. Election stalled, restarting...");
        inElection = false;
        armStaggeredElectionStartTimeout(coordinatorId);
    }

    // =========================================================================
    // Election > timeout arming
    // =========================================================================

    // Arms a fallback timer that restarts the election if SYNCHRONIZATION never arrives.
    // Covers the case where the elected winner crashes before broadcasting SYNC (Hint 2).
    private void armElectionTerminationTimeout() {
        cancel(electionTerminationTimeoutSchedule);
        // time for the election to succesfully travel the entire ring 
        long loopTime = (long) getSystemNumberOfActors() * getMaxLatencyPlusTolerance();
        // time to detect a crashed node in the ring
        long crashDetectTime = 3L *getMaxLatencyPlusTolerance();
        // max number of crashes during election (from assumption of always having quorum)
        long maxCrashes = (getSystemNumberOfActors() - 1L) /2L;
        // node sends win notification to leader + leader sends sync message
        long synchronizationTime = getMaxLatencyPlusTolerance()*2;

        long delay = (loopTime + crashDetectTime *maxCrashes + synchronizationTime)*2; 

        electionTerminationTimeoutSchedule = getContext().system().scheduler().scheduleOnce(
                Duration.create(delay, TimeUnit.MILLISECONDS),
                getSelf(),
                new ElectionTerminationTimeout(),
                getContext().dispatcher(),
                getSelf());
    }

    private void armElectionAckTimeout(int target){
        cancel(electionAckTimeoutSchedule);
        electionAckTimeoutSchedule = getContext().system().scheduler().scheduleOnce(
                // expect max delay is maxlatency * 2, using *3 adds the required margin
                Duration.create(getMaxLatencyPlusTolerance() * 3L, TimeUnit.MILLISECONDS),
                getSelf(),
                new ElectionAckTimeout(target),
                getContext().system().dispatcher(),
                getSelf());
    }


    // =========================================================================
    // Election > succesful election + synchronization
    // =========================================================================

    private void becomeCoordinator() {
        if (id == coordinatorId && !inElection) return;
        coordinatorId = id;
        currentEpoch = Math.max(currentEpoch, lastEpoch()) + 1; // new epoch ensures updates are distinguishable from pre-crash ones
        sequenceNumber = 0;
        inElection = false;
        cancel(heartbeatTimeoutSchedule);
        cancel(electionAckTimeoutSchedule);
        cancel(electionTerminationTimeoutSchedule);
        callbackOnCoordinatorElected(id);
        // Hint 3: commit any updates the old coordinator may have applied before crashing
        // (received UPDATE + sent ACK, but WRITEOK never arrived). Applying them here ensures
        // they appear in the SYNCHRONIZATION history, satisfying uniform agreement.
        List<Update> uncommitted = new ArrayList<>(pendingUpdates.values());
        uncommitted.sort(Comparator.comparingInt((Update u) -> u.epoch).thenComparingInt(u -> u.seqNum));
        for (Update u : uncommitted) {
            if (!historyContains(u.epoch, u.seqNum)) {
                applyUpdate(u);
            }
        }
        broadcastToOthers(new Synchronization(id, currentEpoch, updateHistory));
        for (Cancellable c : forwardTimers.values()) cancel(c);
        forwardTimers.clear();
        for (Cancellable c : updateTimers.values()) cancel(c);
        updateTimers.clear();
        sendHeartbeats();
        scheduleNextHeartbeat();
        // replay writes that arrived during the election so they are not lost
        for (PendingForward pf : new ArrayList<>(pendingForwards)) {
            coordinatorAcceptWrite(pf.index, pf.value, getSelf(), pf.client);
        }
    }

    private void onSynchronization(Synchronization msg) {
        log("Synchronized with new coordinator " + msg.newCoordId + " for epoch " + msg.newEpoch);
        coordinatorId = msg.newCoordId;
        currentEpoch = msg.newEpoch;
        sequenceNumber = 0;
        inElection = false;
        cancel(electionAckTimeoutSchedule);
        cancel(electionTerminationTimeoutSchedule);
        for (Cancellable c : forwardTimers.values()) cancel(c);
            forwardTimers.clear();
        for (Cancellable c : updateTimers.values()) cancel(c);
            updateTimers.clear();
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
            armForwardTimer(pf.id);
        }
    }

    // =========================================================================
    // Crash handling
    // =========================================================================

    private void applyCrashNow() {
        // timer messages would not be handled anyway but we still cancel them
        cancel(heartbeatSchedule);
        cancel(heartbeatTimeoutSchedule);
        cancel(electionAckTimeoutSchedule);
        cancel(electionTerminationTimeoutSchedule);
        for (Cancellable c : forwardTimers.values()) cancel(c);
        forwardTimers.clear();
        for (Cancellable c : updateTimers.values()) cancel(c);
        updateTimers.clear();
        log("CRASHED");
        getContext().become(crashedReceive());
    }

    // Called at the top of every handler that has a crash point.
    // crashes start exactly after the nth message has been handled
    private boolean checkAndApplyCrash(Crash.Type type) {
        if (pendingCrashType == type) {
            pendingCrashCount--;
            if (pendingCrashCount <= 0) {
                applyCrashNow();
                return true;
            }
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

    private void armForwardTimer(long fId) {
        cancel(forwardTimers.get(fId));
        Cancellable c = getContext().system().scheduler().scheduleOnce(
                Duration.create(getMaxLatencyPlusTolerance() * 3L, TimeUnit.MILLISECONDS),
                getSelf(),
                new ForwardWriteTimeoutMsg(fId),
                getContext().system().dispatcher(),
                getSelf());
        forwardTimers.put(fId, c);
    }
}
