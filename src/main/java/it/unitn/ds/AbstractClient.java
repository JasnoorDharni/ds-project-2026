package it.unitn.ds;

import java.io.Serializable;
import java.util.Optional;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;

public abstract class AbstractClient extends AbstractActor {
    private final Optional<ActorRef> defaultTargetReplica;
    private final Optional<ActorRef> listener;
    private final long readTimeoutDelay;
    private final long writeTimeoutDelay;

    AbstractClient(long readTimeoutDelay, long writeTimeoutDelay) {
        this(readTimeoutDelay, writeTimeoutDelay, Optional.empty(), Optional.empty());
    }

    AbstractClient(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> listener, Optional<ActorRef> defaultTargetReplica) {
        this.listener = listener;
        this.defaultTargetReplica = defaultTargetReplica;
        this.readTimeoutDelay = readTimeoutDelay;
        this.writeTimeoutDelay = writeTimeoutDelay;
    }

    public long getReadTimeoutDelay() {
        return readTimeoutDelay;
    }

    public long getWriteTimeoutDelay() {
        return writeTimeoutDelay;
    }

    // =================================================================================
    // API Messages
    // =================================================================================

    public static class ReadRequest {
        ActorRef replica;
        int index;

        public ReadRequest(int index) {
            this(index, null);
        }

        public ReadRequest(int index, ActorRef replica) {
            this.replica = replica;
            this.index = index;
        }
    }

    public static class WriteRequest {
        ActorRef replica;
        int index;
        int value;

        public WriteRequest(int index, int value) {
            this(index, value, null);
        }

        public WriteRequest(int index, int value, ActorRef replica) {
            this.value = value;
            this.index = index;
            this.replica = replica;
        }
    }

    private static class Result implements Serializable {
        public final Boolean success;
        public final int index;
        public final Integer value;
        public final int fromReplica;

        public Result(boolean success, int index, Integer value, int fromReplica) {
            this.success = success;
            this.index = index;
            this.value = value;
            this.fromReplica = fromReplica;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Result) {
                return ((Result)obj).success == this.success && ((Result)obj).value == this.value && ((Result)obj).index == this.index && ((Result)obj).fromReplica == this.fromReplica;
            }
            return super.equals(obj);
        }

        @Override
        public String toString() {
            return "(" + success + ", " + index + ", " + value + ", " + fromReplica + ")";
        }
    }

    public static class ReadResult extends Result {
        public ReadResult(boolean success, int index, Integer value, int fromReplica) {
            super(success, index, value, fromReplica);
        }
    }

    public static class WriteResult extends Result {
        public WriteResult(boolean success, int index, Integer value, int fromReplica) {
            super(success, index, value, fromReplica);
        }
    }

    private static class Timeout implements Serializable {
        public final ActorRef client;
        public final ActorRef replica;
        public final int index;

        public Timeout(ActorRef client, ActorRef replica, int index) {
            this.client = client;
            this.replica = replica;
            this.index = index;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Timeout) {
                return ((Timeout)obj).client.equals(this.client) && ((Timeout)obj).replica.equals(this.replica) && ((Timeout)obj).index == this.index;
            }
            return super.equals(obj);
        }
    }

    public static class ReadTimeout extends Timeout {
        public ReadTimeout(ActorRef client, ActorRef replica, int index) {
            super(client, replica, index);
        }
    }

    public static class WriteTimeout extends Timeout {
        public final int value;
        public WriteTimeout(ActorRef client, ActorRef replica, int index, int value) {
            super(client, replica, index);
            this.value = value;
        }
    }

    // =================================================================================
    // Helper Methods
    // =================================================================================

    void log(String msg) {
        Logger.log("[Client " + getSelf().path().name() + "] " + msg);
    }

    void debug(String msg) {
        Logger.debug("[Client " + getSelf().path().name() + "] " + msg);
    }

    // =================================================================================
    // Mandatory API Callbacks
    // =================================================================================

    /**
     * This function must be invoked whenever the system answers to a READ request
     * 
     * @param readResult The status of system's answer
     */
    final void callbackOnReadResult(AbstractClient.ReadResult readResult) {
        log("READ complete " + readResult);
        listener.ifPresent(l -> l.tell(readResult, getSelf()));
    }

    /**
     * This function must be invoked whenever the system answers to a WRITE request
     * 
     * @param writeResult The status of system's answer
     */
    final void callbackOnWriteResult(AbstractClient.WriteResult writeResult) {
        log("WRITE complete " + writeResult);
        listener.ifPresent(l -> l.tell(writeResult, getSelf()));
    }

    /**
     * This function must be invoked whenever the client senses a timed-out read request
     */
    final void callbackOnReadTimeout(AbstractClient.ReadTimeout timeout) {
        log("TIMEOUT READ request to "+ timeout.replica.path().name() + " (" + timeout.index + ")");
        listener.ifPresent(l -> l.tell(timeout, getSelf()));
    }

    /**
     * This function must be invoked whenever the client senses a timed-out write request
     */
    final void callbackOnWriteTimeout(AbstractClient.WriteTimeout timeout) {
        log("TIMEOUT WRITE request to " + timeout.replica.path().name() + " (" + timeout.index + ", " + timeout.value + ")");
        listener.ifPresent(l -> l.tell(timeout, getSelf()));
    }

    // =================================================================================
    // Wrapper Handlers
    // =================================================================================

    private final void onReadRequest(AbstractClient.ReadRequest msg) throws Exception {
        if (msg.replica != null) {
            sendRead(msg.replica, msg.index);
        } else if (defaultTargetReplica.isPresent()) {
            sendRead(defaultTargetReplica.get(), msg.index);
        } else {
            throw new Exception("Target replica not found: neither in AbstractClient.WriteRequest nor in AbstractClient.defaultTargetReplica.");
        }
    }

    private final void onWriteRequest(AbstractClient.WriteRequest msg) throws Exception {
        if (msg.replica != null) {
            sendWrite(msg.replica, msg.index, msg.value);
        } else if (defaultTargetReplica.isPresent()) {
            sendWrite(defaultTargetReplica.get(), msg.index, msg.value);
        } else {
            throw new Exception("Target replica not found: neither in AbstractClient.WriteRequest nor in AbstractClient.defaultTargetReplica.");
        }
    }

    // =================================================================================
    // Abstract Methods
    // =================================================================================
    /**
     * Send a read request to a specific replica
     * @param replica replica's ActorRef
     * @param index position's index to read
     */
    abstract public void sendRead(ActorRef replica, int index);
    /**
     * Send a write request to a specific replica
     * @param replica replica's ActorRef
     * @param index position's index to write to
     * @param value new position[index] value
     */
    abstract public void sendWrite(ActorRef replica, int index, int value);

    public final ReceiveBuilder createBaseReceiveBuilder() {
        return receiveBuilder()
            .match(AbstractClient.ReadRequest.class, this::onReadRequest)
            .match(AbstractClient.WriteRequest.class, this::onWriteRequest);
    }

}
