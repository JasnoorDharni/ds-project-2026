package it.unitn.ds.extra;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

/**
 * Scenario 2 — non-coordinator crash (Crash.Type.Now).
 * One non-coordinator replica crashes immediately before any write.
 * With N=5 and Q=3, the remaining 4 replicas are more than enough for quorum.
 */
class NonCoordinatorCrashNow {

	private static final int N_NODES = 5;
	private static final int COORD = 0;

	@Test
	void nonCoordinatorCrashNow() throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("nonCoordinatorCrashNow", N_NODES, COORD);

		// Crash a non-coordinator replica before the write is issued
		sys.actors.get(2).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.probes.get(2).fishForMessage(Duration.ofMillis(300), "", msg -> msg instanceof Crash);

		TestKit probe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(1)), probe.getRef()),
				"client");

		client.tell(new AbstractClient.WriteRequest(0, 99), Actor.noSender());
		WriteResult result = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), WriteResult.class);
		assertEquals(true, result.success,
				"Write must still succeed: quorum (3) is met by the 4 surviving replicas");

		sys.system.terminate();
	}
}
