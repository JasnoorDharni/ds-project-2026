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
 * Scenario 3 — coordinator crash (Crash.Type.Now) + ring election.
 * The coordinator crashes immediately. Non-coordinators detect the missing heartbeat
 * after 2x the beat interval and start a ring election. After the new coordinator is
 * elected, writes and reads proceed normally.
 */
class CoordinatorCrashNow {

	private static final int N_NODES = 5;
	private static final int COORD = 0;

	@Test
	void coordinatorCrashNow() throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("coordinatorCrashNow", N_NODES, COORD);

		sys.actors.get(COORD).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.probes.get(COORD).expectMsgClass(Duration.ofMillis(300), Crash.class);

		// Wait for heartbeat timeout + ring election to complete
		Thread.sleep(AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3L);

		TestKit probe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(1)), probe.getRef()),
				"client");

		client.tell(new AbstractClient.WriteRequest(0, 7), Actor.noSender());
		WriteResult writeResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), WriteResult.class);
		assertEquals(true, writeResult.success, "Write must succeed once the new coordinator is elected");

		Thread.sleep(TestsCommons.getBaseMaxUpdateDelay(sys));

		client.tell(new AbstractClient.ReadRequest(0), Actor.noSender());
		ReadResult readResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)), ReadResult.class);
		assertEquals(7, readResult.value, "Read must return the value written after the election");

		sys.system.terminate();
	}
}
