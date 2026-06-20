package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Main {

    private static final int N    = 5;   // N=5 → quorum Q=3; can lose 2 replicas and still commit
    private static final int COORD = 0;

    public static void main(String[] args) throws InterruptedException {
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        scenario1_normalWriteRead();
        scenario2_nonCoordinatorCrashNow();
        scenario3_coordinatorCrashNow();
        scenario4_crashOnUpdate();
        scenario5_crashOnWriteOk();
        scenario6_crashOnElection();
        scenario7_crashOnHeartbeat();
    }

    // =========================================================================
    // Scenario 1 — baseline happy path
    // =========================================================================

    /** Client writes a value and then reads it back. No failures. */
    private static void scenario1_normalWriteRead() throws InterruptedException {
        banner("Scenario 1: normal write + read");
        ActorSystem sys = mkSystem("Scenario1");
        Map<Integer, ActorRef> replicas = mkReplicas(sys);
        ActorRef client = mkClient(sys, replicas.get(1));

        client.tell(new AbstractClient.WriteRequest(0, 42), ActorRef.noSender());
        sleep(500);
        client.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        sleep(500);

        sys.terminate();
    }

    // =========================================================================
    // Scenario 2 — non-coordinator crash (Crash.Type.Now)
    // =========================================================================

    /**
     * One non-coordinator replica crashes immediately before any write.
     * With N=5 and Q=3, the remaining 4 replicas are more than enough for quorum.
     */
    private static void scenario2_nonCoordinatorCrashNow() throws InterruptedException {
        banner("Scenario 2: non-coordinator crash (Now) — quorum still met");
        ActorSystem sys = mkSystem("Scenario2");
        Map<Integer, ActorRef> replicas = mkReplicas(sys);
        ActorRef client = mkClient(sys, replicas.get(1));

        replicas.get(2).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());
        sleep(100);

        client.tell(new AbstractClient.WriteRequest(0, 99), ActorRef.noSender());
        sleep(500);

        sys.terminate();
    }

    // =========================================================================
    // Scenario 3 — coordinator crash (Crash.Type.Now) + ring election
    // =========================================================================

    /**
     * The coordinator crashes immediately. Non-coordinators detect the missing heartbeat
     * after 2× the beat interval and start a ring election. After the new coordinator is
     * elected, writes and reads proceed normally.
     */
    private static void scenario3_coordinatorCrashNow() throws InterruptedException {
        banner("Scenario 3: coordinator crash (Now) → ring election → write + read");
        ActorSystem sys = mkSystem("Scenario3");
        Map<Integer, ActorRef> replicas = mkReplicas(sys);
        ActorRef client = mkClient(sys, replicas.get(1));

        replicas.get(COORD).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());
        sleep(AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3L); // wait for heartbeat timeout + election

        client.tell(new AbstractClient.WriteRequest(0, 7), ActorRef.noSender());
        sleep(500);
        client.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        sleep(500);

        sys.terminate();
    }

    // =========================================================================
    // Scenario 4 — deferred crash on UPDATE (Crash.Type.Update)
    // =========================================================================

    /**
     * Replica 3 is instructed to crash before processing its 2nd UPDATE message
     * (after_n=1 → processes 1 normally, crashes before the next).
     *
     * First write:  all 5 replicas send ACK → coordinator commits normally.
     * Second write: replica 3 crashes before ACKing → coordinator reaches quorum
     *               from replicas 0,1,2,4 (4 ACKs ≥ Q=3) → write still commits.
     *
     * Demonstrates: 2PC tolerates missing ACKs as long as quorum is reached.
     */
    private static void scenario4_crashOnUpdate() throws InterruptedException {
        banner("Scenario 4: crash on UPDATE (after_n=1) — 2nd write loses one ACK, quorum still met");
        ActorSystem sys = mkSystem("Scenario4");
        Map<Integer, ActorRef> replicas = mkReplicas(sys);
        ActorRef client = mkClient(sys, replicas.get(1));

        replicas.get(3).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Update, 1), ActorRef.noSender());

        client.tell(new AbstractClient.WriteRequest(5, 11), ActorRef.noSender()); // 1st write: all replicas participate
        sleep(300);
        client.tell(new AbstractClient.WriteRequest(5, 22), ActorRef.noSender()); // 2nd write: replica 3 crashes on UPDATE
        sleep(500);
        client.tell(new AbstractClient.ReadRequest(5), ActorRef.noSender());
        sleep(500);

        sys.terminate();
    }

    // =========================================================================
    // Scenario 5 — deferred crash on WRITEOK (Crash.Type.WriteOK)
    // =========================================================================

    /**
     * Replica 3 is instructed to crash before applying its 2nd WRITEOK
     * (after_n=1 → applies 1 normally, crashes before the next).
     *
     * First write:  all replicas apply the commit (replica 3 applies its 1st WriteOK).
     * Second write: replica 3 crashes before applying → its positions[] stays stale,
     *               but the commit is still valid (replicas 0,1,2,4 all applied it).
     *
     * Then the coordinator crashes → ring election among 1,2,4 → new coordinator
     * broadcasts SYNCHRONIZATION with the full committed history. This is the mechanism
     * by which a replica that missed a WRITEOK would catch up upon recovery.
     */
    private static void scenario5_crashOnWriteOk() throws InterruptedException {
        banner("Scenario 5: crash on WRITEOK (after_n=1) → stale replica; coordinator crash → SYNCHRONIZATION");
        ActorSystem sys = mkSystem("Scenario5");
        Map<Integer, ActorRef> replicas = mkReplicas(sys);
        ActorRef client = mkClient(sys, replicas.get(1));

        replicas.get(3).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.WriteOK, 1), ActorRef.noSender());

        client.tell(new AbstractClient.WriteRequest(10, 55), ActorRef.noSender()); // 1st write: replica 3 applies normally
        sleep(300);
        client.tell(new AbstractClient.WriteRequest(10, 66), ActorRef.noSender()); // 2nd write: replica 3 crashes before applying
        sleep(500);

        // Crash coordinator → election; new coordinator sends SYNCHRONIZATION with committed history
        replicas.get(COORD).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());
        sleep(AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3L);

        client.tell(new AbstractClient.WriteRequest(10, 77), ActorRef.noSender());
        sleep(500);
        client.tell(new AbstractClient.ReadRequest(10), ActorRef.noSender());
        sleep(500);

        sys.terminate();
    }

    // =========================================================================
    // Scenario 6 — crash during ring election (Crash.Type.Election)
    // =========================================================================

    /**
     * The coordinator and replica 2 both crash. When the surviving replicas start the
     * ring election, they try to send the ELECTION message to replica 2 (ring order),
     * receive no ElectionAck, hit the ElectionAckTimeout, skip replica 2, and continue
     * the ring to the next live replica. Election completes; write + read succeed.
     *
     * Note: replica 2 is crashed with Crash.Type.Now immediately after the coordinator,
     * which simulates a replica that goes silent mid-ring (no ElectionAck returned).
     */
    private static void scenario6_crashOnElection() throws InterruptedException {
        banner("Scenario 6: crash during election — ring skips silent hop via ElectionAckTimeout");
        ActorSystem sys = mkSystem("Scenario6");
        Map<Integer, ActorRef> replicas = mkReplicas(sys);
        ActorRef client = mkClient(sys, replicas.get(1));

        replicas.get(COORD).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());
        replicas.get(2).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), ActorRef.noSender());

        // heartbeat detection (2× beat) + ring hops + ElectionAckTimeout for skipped replica 2
        sleep(AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3L + AbstractReplica.MAX_LATENCY * 20L);

        client.tell(new AbstractClient.WriteRequest(0, 33), ActorRef.noSender());
        sleep(500);
        client.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        sleep(500);

        sys.terminate();
    }

    // =========================================================================
    // Scenario 7 — deferred crash on HEARTBEAT (Crash.Type.Heartbeat)
    // =========================================================================

    /**
     * Replica 3 is instructed to crash before processing its 3rd HEARTBEAT
     * (after_n=2 → processes 2 normally, crashes before the next).
     * After ~3 beat intervals it silently disappears. Since it is not the coordinator,
     * no election is triggered; the remaining 4 replicas (0,1,2,4) satisfy quorum (Q=3)
     * and the system continues normally.
     *
     * Demonstrates: a silent non-coordinator does not disrupt the system; only a
     * coordinator failure triggers an election.
     */
    private static void scenario7_crashOnHeartbeat() throws InterruptedException {
        banner("Scenario 7: crash on HEARTBEAT (after_n=2) — silent non-coordinator; system unaffected");
        ActorSystem sys = mkSystem("Scenario7");
        Map<Integer, ActorRef> replicas = mkReplicas(sys);
        ActorRef client = mkClient(sys, replicas.get(1));

        replicas.get(3).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Heartbeat, 2), ActorRef.noSender());

        // Wait for replica 3 to receive and process 2 heartbeats, then crash on the 3rd
        sleep(AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3L);

        // Replicas 0,1,2,4 are alive → quorum met → write + read succeed
        client.tell(new AbstractClient.WriteRequest(0, 88), ActorRef.noSender());
        sleep(500);
        client.tell(new AbstractClient.ReadRequest(0), ActorRef.noSender());
        sleep(500);

        sys.terminate();
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static ActorSystem mkSystem(String name) {
        return ActorSystem.create(name);
    }

    private static Map<Integer, ActorRef> mkReplicas(ActorSystem sys) throws InterruptedException {
        Map<Integer, ActorRef> replicas = new HashMap<>(N);
        for (int i = 0; i < N; i++) {
            replicas.put(i, sys.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                            AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i));
        }
        InitSystem init = new InitSystem(replicas, COORD);
        for (ActorRef r : replicas.values()) r.tell(init, ActorRef.noSender());
        sleep(100); // let actors initialize before injecting faults
        return replicas;
    }

    private static ActorRef mkClient(ActorSystem sys, ActorRef defaultReplica) {
        long readTimeout  = AbstractReplica.MAX_LATENCY * N * 8L;
        long writeTimeout = readTimeout + AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3L * 5;
        return sys.actorOf(Client.props(readTimeout, writeTimeout, Optional.of(defaultReplica)), "Client");
    }

    private static void banner(String title) {
        System.out.println("\n========================================");
        System.out.println(title);
        System.out.println("========================================\n");
    }

    private static void sleep(long ms) throws InterruptedException {
        Thread.sleep(ms);
    }
}
