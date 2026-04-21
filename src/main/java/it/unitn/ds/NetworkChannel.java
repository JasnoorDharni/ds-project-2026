package it.unitn.ds;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

/**
 * A channel actor that delivers messages to a fixed destination with random
 * delay, but preserving FIFO order. One instance per (sender, destination)
 * pair.
 */
public class NetworkChannel extends AbstractActor {

    // Sent to self to trigger delivery of the head of the queue
    private static class Deliver implements Serializable {
    }

    private final ActorRef destination;
    private final int minLatency;
    private final int maxLatency;
    private final Random rnd;

    // Queue of (message, originalSender) pairs waiting to be delivered
    private final Queue<Object[]> queue = new LinkedList<>();
    private boolean delivering = false;

    private NetworkChannel(ActorRef destination, int minLatency, int maxLatency) {
        this.destination = destination;
        this.minLatency = minLatency;
        this.maxLatency = maxLatency;
        this.rnd = new Random();
    }

    public static Props props(ActorRef destination, int minLatency, int maxLatency) {
        return Props.create(NetworkChannel.class,
                () -> new NetworkChannel(destination, minLatency, maxLatency));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Deliver.class, msg -> onDeliver())
                .matchAny(msg -> onEnqueue(msg, getSender()))
                .build();
    }

    private void onEnqueue(Object msg, ActorRef sender) {
        queue.add(new Object[] { msg, sender });
        if (!delivering) {
            scheduleNextDelivery();
        }
    }

    private void onDeliver() {
        Object[] entry = queue.poll();
        if (entry == null) {
            delivering = false;
            return;
        }
        Object msg = entry[0];
        ActorRef originalSender = (ActorRef) entry[1];
        destination.tell(msg, originalSender);

        if (!queue.isEmpty()) {
            scheduleNextDelivery();
        } else {
            delivering = false;
        }
    }

    private void scheduleNextDelivery() {
        delivering = true;
        int delay = minLatency + rnd.nextInt(maxLatency - minLatency);
        getContext().system().scheduler().scheduleOnce(
                Duration.create(delay, TimeUnit.MILLISECONDS),
                getSelf(),
                new Deliver(),
                getContext().system().dispatcher(),
                getSelf());
    }
}