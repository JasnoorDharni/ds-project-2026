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
 * Scenario 7 — deferred crash on HEARTBEAT (Crash.Type.Heartbeat).
 *
 * Replica 3 is instructed to crash before processing its 3rd HEARTBEAT
 * (after_n=2 -> processes 2 normally, crashes before the next).
 * After ~3 beat intervals it silently disappears. Since it is not the coordinator,
 * no election is triggered; the remaining 4 replicas (0,1,2,4) satisfy quorum (Q=3)
 * and the system continues normally.
 *
 * Demonstrates: a silent non-coordinator does not disrupt the system; only a
 * coordinator failure triggers an election.
 */
class CrashOnHeartbeat {

	private static final int N_NODES = 5;
	private static final int COORD = 0;

	@Test
	void crashOnHeartbeat() throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("crashOnHeartbeat", N_NODES, COORD);

		sys.actors.get(3).tell(new Crash(Crash.Type.Heartbeat, 2), Actor.noSender());

		// Wait for replica 3 to receive and process 2 heartbeats, then crash on the 3rd
		Thread.sleep(AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3L);

		TestKit probe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(1)), probe.getRef()),
				"client");

		// Replicas 0,1,2,4 are alive -> quorum met -> write + read succeed
		client.tell(new AbstractClient.WriteRequest(0, 88), Actor.noSender());
		WriteResult writeResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), WriteResult.class);
		assertEquals(true, writeResult.success,
				"Write must succeed: a silent non-coordinator does not disrupt quorum");

		Thread.sleep(TestsCommons.getBaseMaxUpdateDelay(sys));

		client.tell(new AbstractClient.ReadRequest(0), Actor.noSender());
		ReadResult readResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)), ReadResult.class);
		assertEquals(88, readResult.value, "Read must return the value written while replica 3 is silent");

		sys.system.terminate();
	}
}
