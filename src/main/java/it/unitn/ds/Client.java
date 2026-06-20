package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

// Akka actor that represents a client of the distributed replica system.
// Sends Read/Write messages to replicas, arms a per-operation timeout timer, and on
// response (or timeout) fires the appropriate AbstractClient callback for the test framework.
public class Client extends AbstractClient {

    // All maps below are keyed by array index; at most one pending read/write per position at a time.
    private final Map<Integer, Cancellable> pendingReadTimers = new HashMap<>();
    private final Map<Integer, Cancellable> pendingWriteTimers = new HashMap<>();
    // Retains the value sent so WriteTimeout can report exactly what was attempted.
    private final Map<Integer, Integer> pendingWriteValues = new HashMap<>();

    Client(long readTimeoutDelay, long writeTimeoutDelay,
           Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay,
                              Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class,
                () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay,
                                          Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class,
                () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    // =========================================================================
    // Send methods
    // =========================================================================

    @Override
    public void sendRead(ActorRef replica, int index) {
        log("requesting READ (" + index + ") to " + replica.path().name());
        replica.tell(new Replica.ReadRequest(index), getSelf());
        cancel(pendingReadTimers.remove(index)); // cancel any stale timer if client retries the same index
        Cancellable t = getContext().system().scheduler().scheduleOnce(
                Duration.create(getReadTimeoutDelay(), TimeUnit.MILLISECONDS),
                getSelf(),
                new AbstractClient.ReadTimeout(getSelf(), replica, index),
                getContext().dispatcher(),
                ActorRef.noSender());
        pendingReadTimers.put(index, t);
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        log("requesting WRITE (" + index + ", " + value + ") to " + replica.path().name());
        replica.tell(new Replica.WriteRequest(index, value), getSelf());
        pendingWriteValues.put(index, value);
        cancel(pendingWriteTimers.remove(index)); // cancel any stale timer if client retries the same index
        Cancellable t = getContext().system().scheduler().scheduleOnce(
                Duration.create(getWriteTimeoutDelay(), TimeUnit.MILLISECONDS),
                getSelf(),
                new AbstractClient.WriteTimeout(getSelf(), replica, index, value),
                getContext().dispatcher(),
                ActorRef.noSender());
        pendingWriteTimers.put(index, t);
    }

    // =========================================================================
    // Receive
    // =========================================================================

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(Replica.ReadResponse.class, this::onReadResponse)
                .match(Replica.WriteResponse.class, this::onWriteResponse)
                .match(AbstractClient.ReadTimeout.class, this::onReadTimeout)
                .match(AbstractClient.WriteTimeout.class, this::onWriteTimeout)
                .build();
    }

    private void onReadResponse(Replica.ReadResponse msg) {
        cancel(pendingReadTimers.remove(msg.index));
        callbackOnReadResult(new ReadResult(true, msg.index, msg.value, msg.replicaId));
    }

    private void onWriteResponse(Replica.WriteResponse msg) {
        cancel(pendingWriteTimers.remove(msg.index));
        pendingWriteValues.remove(msg.index);
        callbackOnWriteResult(new WriteResult(msg.success, msg.index, msg.value, msg.replicaId));
    }

    private void onReadTimeout(AbstractClient.ReadTimeout msg) {
        if (!pendingReadTimers.containsKey(msg.index)) return; // response already arrived; discard stale timeout
        pendingReadTimers.remove(msg.index);
        callbackOnReadTimeout(msg);
    }

    private void onWriteTimeout(AbstractClient.WriteTimeout msg) {
        if (!pendingWriteTimers.containsKey(msg.index)) return; // response already arrived; discard stale timeout
        pendingWriteTimers.remove(msg.index);
        pendingWriteValues.remove(msg.index);
        callbackOnWriteTimeout(msg);
    }

    private void cancel(Cancellable c) {
        if (c != null && !c.isCancelled()) c.cancel();
    }
}
