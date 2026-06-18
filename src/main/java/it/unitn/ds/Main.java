package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        final int N_REPLICAS = 4;
        final int COORDINATOR_ID = 0;
        final ActorSystem system = ActorSystem.create("TestMain");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                                  AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i
                )
            );
        }

        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }

        final long readTimeout  = AbstractReplica.MAX_LATENCY * N_REPLICAS * 8L;
        final long writeTimeout = readTimeout + (AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3L) * 5;

        ActorRef client = system.actorOf(
                Client.props(readTimeout, writeTimeout, Optional.of(replicas.get(1))),
                "Client_1");

        // Scenario 1: write + read
        client.tell(new AbstractClient.WriteRequest(0, 42), ActorRef.noSender());
        Thread.sleep(500);
        client.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        Thread.sleep(500);

        // Scenario 2: crash a non-coordinator replica, write still succeeds
        replicas.get(2).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());
        Thread.sleep(200);
        client.tell(new AbstractClient.WriteRequest(0, 99), ActorRef.noSender());
        Thread.sleep(500);

        // Scenario 3: crash the coordinator, election should elect a new one
        replicas.get(COORDINATOR_ID).tell(
                new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());
        Thread.sleep(AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3L);
        client.tell(new AbstractClient.WriteRequest(0, 7), ActorRef.noSender());
        Thread.sleep(500);
        client.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        Thread.sleep(500);

        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }
}
