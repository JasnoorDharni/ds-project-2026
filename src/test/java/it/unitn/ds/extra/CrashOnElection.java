package it.unitn.ds.extra;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

/**
 * Scenario 6 — crash during ring election (Crash.Type.Now on two replicas).
 *
 * The coordinator and replica 2 both crash. When the surviving replicas start the
 * ring election, they try to send the ELECTION message to replica 2 (ring order),
 * receive no ElectionAck, hit the ElectionAckTimeout, skip replica 2, and continue
 * the ring to the next live replica. Election completes; write + read succeed.
 *
 * Note: replica 2 is crashed with Crash.Type.Now immediately after the coordinator,
 * which simulates a replica that goes silent mid-ring (no ElectionAck returned).
 */
class CrashOnElection {

	private static final int N_NODES = 5;
	private static final int COORD = 0;

	@Test
	void crashOnElection() throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("crashOnElection", N_NODES, COORD);

		sys.actors.get(COORD).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.actors.get(2).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.probes.get(COORD).fishForMessage(Duration.ofMillis(300), "", msg -> msg instanceof Crash);
		sys.probes.get(2).fishForMessage(Duration.ofMillis(300), "", msg -> msg instanceof Crash);

		// heartbeat detection (2x beat) + ring hops + ElectionAckTimeout for skipped replica 2
		Thread.sleep(AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3L + AbstractReplica.MAX_LATENCY * 20L);

		TestKit probe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(1)), probe.getRef()),
				"client");

		client.tell(new AbstractClient.WriteRequest(0, 33), Actor.noSender());
		WriteResult writeResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), WriteResult.class);
		assertEquals(true, writeResult.success,
				"Write must succeed: the ring election skips the silent replica 2 and still elects a coordinator");

		Thread.sleep(TestsCommons.getBaseMaxUpdateDelay(sys));

		client.tell(new AbstractClient.ReadRequest(0), Actor.noSender());
		ReadResult readResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)), ReadResult.class);
		assertEquals(33, readResult.value, "Read must return the value committed after the election");

		sys.system.terminate();
	}
}
