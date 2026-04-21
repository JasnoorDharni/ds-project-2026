package it.unitn.ds;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;

public abstract class AbstractReplica extends AbstractActor {
    // === Constants ===
    public static final int MIN_LATENCY = 5;
    public static final int MAX_LATENCY = 20;
    public static final int COORDINATOR_BEAT_INTERVAL = 1000;
    public static final int POSITIONS_LIST_LENGTH = 100;

    // === Local Data ===
    final int id;
    boolean initialized;

    // === Network Simulation ===
    // Min and Max latency millis
    private int minLatency;
    private int maxLatency;
    private int coordinatorBeatInterval;
    private final Map<ActorRef, ActorRef> channels = new HashMap<>();

    // === Tests ===
    private final Optional<ActorRef> listener;

    AbstractReplica(int id) {
        this(id, MIN_LATENCY, MAX_LATENCY, COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    AbstractReplica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        this.id = id;
        this.coordinatorBeatInterval = coordinatorBeatInterval;
        this.listener = listener;
        setNetworkLatency(minLatency, maxLatency);
    }

    // =================================================================================
    // Getters and Setters
    // =================================================================================

    /**
     * 
     * @return coordinator beat interval in milliseconds
     */
    public int getCoordinatorBeatInterval() {
        return coordinatorBeatInterval;
    }

    /**
     * 
     * @param min_latency in milliseconds
     * @param max_latency in milliseconds
     */
    public void setNetworkLatency(int min_latency, int max_latency) {
        this.minLatency = min_latency;
        this.maxLatency = max_latency;
    }

    /**
     * 
     * @return minimum latency in milliseconds
     */
    public int getMinLatency() {
        return minLatency;
    }

    /**
     * 
     * @return maximum latency in milliseconds
     */
    public int getMaxLatency() {
        return maxLatency;
    }

    /**
     * 
     * @return maximum latency + tolerance (based on the number of replicas) in
     *         milliseconds
     */
    public int getMaxLatencyPlusTolerance() {
        return maxLatency + (int)((float)maxLatency/2.0 * getSystemNumberOfActors());
    }

    // =================================================================================
    // Network Emulation
    // =================================================================================

    void tell(Serializable m, ActorRef dst) {
        // Lazily create one channel actor per destination
        ActorRef channel = channels.computeIfAbsent(dst, d -> getContext().actorOf(
                NetworkChannel.props(d, getMinLatency(), getMaxLatency()),
                "channel_to_" + d.path().name()));
        channel.tell(m, getSelf());
    }

    // =================================================================================
    // Helper Methods
    // =================================================================================

    void log(String msg) {
        Logger.log("[Replica " + id + "] " + msg);
    }

    void debug(String msg) {
        Logger.debug("[Replica " + id + "] " + msg);
    }

    // =================================================================================
    // API Messages
    // =================================================================================

    /**
     * Encapsulates the initialization data required to set up a replica system.
     * <p>
     * This includes the full group of replicas participating in the system and
     * the identifier of the coordinator replica.
     * </p>
     *
     * <p>
     * The {@code group} map is made unmodifiable to ensure immutability after
     * construction.
     * </p>
     */
    public static class InitSystem implements Serializable {

        /**
         * Mapping from replica identifiers to their corresponding {@link ActorRef}.
         */
        public final Map<Integer, ActorRef> group;

        /**
         * The identifier of the coordinator replica within the group.
         */
        public final int coordinator_id;

        /**
         * Constructs a new {@code InitSystem} object.
         *
         * @param group          a mapping of replica IDs to their actor references
         * @param coordinator_id the ID of the coordinator replica
         */
        public InitSystem(Map<Integer, ActorRef> group, int coordinator_id) {
            this.group = Collections.unmodifiableMap(new HashMap<>(group));
            this.coordinator_id = coordinator_id;
        }
    }

    /**
     * Represents a crash configuration for a component in the system.
     * <p>
     * A {@code Crash} object specifies when a crash should occur based on
     * the type of message being processed and how many such messages have
     * been handled before the crash is triggered.
     * </p>
     *
     * <p>
     * This class is immutable and serializable.
     * </p>
     */
    public static class Crash implements Serializable {

        /**
         * Enumeration of message types that can trigger a crash.
         */
        public enum Type {
            /**
             * Crash immediately.
             */
            Now,

            /**
             * Crash after processing heartbeat messages.
             */
            Heartbeat,

            /**
             * Crash after processing update messages.
             */
            Update,

            /**
             * Crash after processing write acknowledgment messages.
             */
            WriteOK,

            /**
             * Crash after processing election-related messages.
             */
            Election
        }

        /**
         * The type of message that determines when the crash will occur.
         */
        public final Crash.Type type;

        /**
         * The number of messages of the specified {@link #type} to process
         * before triggering the crash.
         * <p>
         * After this threshold is reached, the component is expected to crash,
         * meaning it will stop responding to any further messages.
         * </p>
         */
        public final int after_n_messages_of_type;

        /**
         * Constructs a new {@code Crash} configuration.
         *
         * @param type                     the type of message that triggers the crash
         *                                 condition
         * @param after_n_messages_of_type the number of messages of the given type
         *                                 to process before crashing
         */
        public Crash(Crash.Type type, int after_n_messages_of_type) {
            this.type = type;
            this.after_n_messages_of_type = after_n_messages_of_type;
        }

        /**
         * Compares this {@code Crash} object with another object for equality.
         * <p>
         * Two {@code Crash} instances are considered equal if they have the same
         * {@link #type} and the same {@link #after_n_messages_of_type}.
         * </p>
         *
         * @param obj the object to compare with
         * @return {@code true} if the given object is equal to this instance;
         *         {@code false} otherwise
         */
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Crash) {
                Crash other = (Crash) obj;
                return this.type == other.type &&
                        this.after_n_messages_of_type == other.after_n_messages_of_type;
            }
            return false;
        }
    }

    public static class CoordinatorElected implements Serializable {
        public final int newCoordinatorId;
        public final int replicaId;

        public CoordinatorElected(int newCoordinatorId, int replicaId) {
            this.newCoordinatorId = newCoordinatorId;
            this.replicaId = replicaId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CoordinatorElected) {
                CoordinatorElected o = (CoordinatorElected) obj;
                return o.newCoordinatorId == this.newCoordinatorId && o.replicaId == this.replicaId;
            }
            return false;
        }

        @Override
        public String toString() {
            return "CoordinatorElected(newCoord=" + newCoordinatorId + ", replica=" + replicaId + ")";
        }
    }

    public static class UpdateApplied implements Serializable {
        public final int replicaId;
        public final int index;
        public final int value;

        public UpdateApplied(int replicaId, int index, int value) {
            this.replicaId = replicaId;
            this.index = index;
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof UpdateApplied) {
                UpdateApplied o = (UpdateApplied) obj;
                return o.replicaId == this.replicaId && o.index == this.index && o.value == this.value;
            }
            return false;
        }

        @Override
        public String toString() {
            return "UpdateApplied(replica=" + replicaId + ", index=" + index + ", value=" + value + ")";
        }
    }

    public static class ElectionStarted implements Serializable {
        public final int replicaId;
        public final int crashedCoordinatorId;

        public ElectionStarted(int replicaId, int crashedCoordinatorId) {
            this.replicaId = replicaId;
            this.crashedCoordinatorId = crashedCoordinatorId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ElectionStarted) {
                ElectionStarted o = (ElectionStarted) obj;
                return o.replicaId == this.replicaId && o.crashedCoordinatorId == this.crashedCoordinatorId;
            }
            return false;
        }

        @Override
        public String toString() {
            return "ElectionStarted(replica=" + replicaId + ", crashedCoord=" + crashedCoordinatorId + ")";
        }
    }

    // =================================================================================
    // Mandatory API Callbacks
    // =================================================================================

    /**
     * Must be invoked whenever this replica recognises a new coordinator.
     * Call this both when:
     * - the replica IS the new coordinator (after deciding it won the election),
     * and
     * - the replica receives a Synchronization message from the new coordinator.
     *
     * @param newCoordinatorId the id of the newly elected coordinator
     */
    final void callbackOnCoordinatorElected(int newCoordinatorId) {
        log("NEW COORDINATOR elected: " + newCoordinatorId);
        listener.ifPresent(l -> l.tell(new CoordinatorElected(newCoordinatorId, this.id), getSelf()));
    }

    /**
     * Must be invoked whenever this replica applies an update to its local state
     * (i.e. right after updating positions[]).
     *
     * @param index the position index that was updated
     * @param value the new value written at that index
     */
    final void callbackOnUpdateApplied(int index, int value) {
        log("UPDATE APPLIED index=" + index + " value=" + value);
        listener.ifPresent(l -> l.tell(new UpdateApplied(this.id, index, value), getSelf()));
    }

    /**
     * Must be invoked whenever this replica starts or joins a coordinator election.
     * Call this exactly once per election participation.
     *
     * @param crashedCoordinatorId the id of the coordinator whose crash triggered
     *                             this election
     */
    final void callbackOnElectionStarted(int crashedCoordinatorId) {
        log("ELECTION STARTED for crashed coordinator: " + crashedCoordinatorId);
        listener.ifPresent(l -> l.tell(new ElectionStarted(this.id, crashedCoordinatorId), getSelf()));
    }

    // =================================================================================
    // Wrapper Handlers
    // =================================================================================

    private final void onCrashMsg(Crash crash) {
        crash(crash);
        log("CRASHED: " + crash.type + " (" + crash.after_n_messages_of_type + ")");
        listener.ifPresent(l -> l.tell(crash, getSelf()));
    }

    private final void onInitSystem(InitSystem msg) {
        initSystem(msg);
        initialized = true;
    }

    // =================================================================================
    // Base Message-Callback bindings
    // =================================================================================

    final ReceiveBuilder createBaseReceiveBuilder() {
        ReceiveBuilder builder = receiveBuilder().match(Crash.class, this::onCrashMsg);
        if (!initialized) {
            builder.match(InitSystem.class, this::onInitSystem);
        }
        return builder;
    }

    // =================================================================================
    // Abstract Methods
    // =================================================================================

    /**
     * 
     * @return Total number of replicas in the system (including this instance)
     */
    abstract public int getSystemNumberOfActors();

    /**
     * Triggers a crash in the replica according to the specified configuration.
     * <p>
     * The behavior of the crash is defined by the provided
     * {@link AbstractReplica.Crash}
     * object, which determines when and how the replica should stop responding
     * to incoming messages.
     * </p>
     *
     * @param how_to_crash the crash configuration describing the type of messages
     *                     to monitor and how many of them should be processed
     *                     before the crash is triggered
     */
    abstract public void crash(AbstractReplica.Crash how_to_crash);

    /**
     * Initializes the replica with system-wide configuration.
     * <p>
     * This method provides the replica with the full set of participants in the
     * system and identifies the coordinator. It is typically called once during
     * system setup before normal operation begins.
     * </p>
     *
     * @param sysInit the initialization data containing the replica group and
     *                coordinator identifier
     */
    abstract public void initSystem(AbstractReplica.InitSystem sysInit);
}
