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

    public static class Read implements Serializable {
        public final int index;
        public Read(int index) { this.index = index; }
    }

    public static class ReadResponse implements Serializable {
        public final int index;
        public final int value;
        public final int replicaId;
        public ReadResponse(int index, int value, int replicaId) {
            this.index = index; this.value = value; this.replicaId = replicaId;
        }
    }

    public static class Write implements Serializable {
        public final int index;
        public final int value;
        public Write(int index, int value) { this.index = index; this.value = value; }
    }

    public static class WriteResponse implements Serializable {
        public final boolean success;
        public final int index;
        public final int value;
        public final int replicaId;
        public WriteResponse(boolean success, int index, int value, int replicaId) {
            this.success = success; this.index = index; this.value = value; this.replicaId = replicaId;
        }
    }

    public static class ForwardWrite implements Serializable {
        public final int index;
        public final int value;
        public final ActorRef originalClient;
        public ForwardWrite(int index, int value, ActorRef originalClient) {
            this.index = index; this.value = value; this.originalClient = originalClient;
        }
    }

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

    public static class Ack implements Serializable {
        public final int epoch;
        public final int seqNum;
        public Ack(int epoch, int seqNum) { this.epoch = epoch; this.seqNum = seqNum; }
    }

    public static class WriteOk implements Serializable {
        public final int epoch;
        public final int seqNum;
        public WriteOk(int epoch, int seqNum) { this.epoch = epoch; this.seqNum = seqNum; }
    }

    public static class Heartbeat implements Serializable {}
    private static class BroadcastHeartbeat implements Serializable {}
    private static class HeartbeatTimeout implements Serializable {}

    public static class ElectionEntry implements Serializable {
        public final int replicaId;
        public final int epoch;
        public final int seqNum;
        public ElectionEntry(int replicaId, int epoch, int seqNum) {
            this.replicaId = replicaId; this.epoch = epoch; this.seqNum = seqNum;
        }
    }

    public static class Election implements Serializable {
        public final List<ElectionEntry> entries;
        public final int crashedCoordId;
        public Election(List<ElectionEntry> entries, int crashedCoordId) {
            this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
            this.crashedCoordId = crashedCoordId;
        }
    }

    public static class ElectionAck implements Serializable {}

    private static class ElectionAckTimeout implements Serializable {
        public final int targetId;
        public ElectionAckTimeout(int targetId) { this.targetId = targetId; }
    }

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
    private final Map<Long, Update> pendingUpdates = new HashMap<>();
    private final Map<Long, Set<Integer>> pendingAcks = new HashMap<>();
    private final List<PendingForward> pendingForwards = new ArrayList<>();

    private boolean crashed = false;
    private Crash.Type pendingCrashType = null;
    private int pendingCrashCount = 0;

    private Cancellable heartbeatSchedule;
    private Cancellable heartbeatTimeoutSchedule;
    private Cancellable electionAckTimeoutSchedule;

    private boolean inElection = false;
    private int electionCrashedCoordId = -1;
    private Election currentElection;
    private final Set<Integer> electionSkipped = new HashSet<>();
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
                .match(Read.class, this::onRead)
                .match(Write.class, this::onWrite)
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

    private void onRead(Read msg) {
        if (crashed) return;
        tell(new ReadResponse(msg.index, positions[msg.index], id), getSender());
    }

    private void onWrite(Write msg) {
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
        if (id != coordinatorId) return;
        coordinatorAcceptWrite(msg.index, msg.value, getSender(), msg.originalClient);
    }

    private void coordinatorAcceptWrite(int index, int value, ActorRef originReplica, ActorRef originClient) {
        int seqNum = sequenceNumber++;
        Update update = new Update(currentEpoch, seqNum, index, value, originReplica, originClient);
        long k = key(currentEpoch, seqNum);
        pendingUpdates.put(k, update);
        Set<Integer> acks = new HashSet<>();
        acks.add(id);
        pendingAcks.put(k, acks);
        broadcastToOthers(update);
        if (acks.size() >= quorum()) {
            broadcastWriteOk(update);
            pendingAcks.remove(k);
        }
    }

    private void onUpdate(Update msg) {
        if (checkAndApplyCrash(Crash.Type.Update)) return;
        long k = key(msg.epoch, msg.seqNum);
        if (!pendingUpdates.containsKey(k)) {
            pendingUpdates.put(k, msg);
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
            pendingAcks.remove(k);
        }
    }

    private void broadcastWriteOk(Update update) {
        broadcastToOthers(new WriteOk(update.epoch, update.seqNum));
        applyUpdate(update);
    }

    private void onWriteOk(WriteOk msg) {
        if (checkAndApplyCrash(Crash.Type.WriteOK)) return;
        long k = key(msg.epoch, msg.seqNum);
        Update u = pendingUpdates.get(k);
        if (u == null) return;
        if (historyContains(u.epoch, u.seqNum)) return;
        applyUpdate(u);
    }

    private void applyUpdate(Update u) {
        positions[u.index] = u.value;
        updateHistory.add(u);
        pendingUpdates.remove(key(u.epoch, u.seqNum));
        callbackOnUpdateApplied(u.index, u.value);
        if (u.originReplica.equals(getSelf())) {
            tell(new WriteResponse(true, u.index, u.value, id), u.originClient);
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
        tell(new ElectionAck(), getSender());

        boolean alreadyInRing = false;
        for (ElectionEntry e : msg.entries) {
            if (e.replicaId == id) { alreadyInRing = true; break; }
        }

        if (alreadyInRing) {
            ElectionEntry winner = computeWinner(msg.entries);
            if (winner.replicaId == id) {
                becomeCoordinator();
            } else {
                tell(msg, group.get(winner.replicaId));
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
        currentEpoch = Math.max(currentEpoch, lastEpoch()) + 1;
        sequenceNumber = 0;
        inElection = false;
        cancel(heartbeatTimeoutSchedule);
        cancel(electionAckTimeoutSchedule);
        callbackOnCoordinatorElected(id);
        broadcastToOthers(new Synchronization(id, currentEpoch, updateHistory));
        sendHeartbeats();
        scheduleNextHeartbeat();
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
        for (Update u : msg.history) {
            if (!historyContains(u.epoch, u.seqNum)) {
                positions[u.index] = u.value;
                updateHistory.add(u);
                pendingUpdates.remove(key(u.epoch, u.seqNum));
                callbackOnUpdateApplied(u.index, u.value);
            }
        }
        scheduleHeartbeatTimeout();
        callbackOnCoordinatorElected(msg.newCoordId);
        for (PendingForward pf : pendingForwards) {
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

    private long key(int epoch, int seqNum) {
        return ((long) epoch << 32) | (seqNum & 0xFFFFFFFFL);
    }

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
