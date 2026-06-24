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
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

/**
 * Scenario 4 — deferred crash on UPDATE (Crash.Type.Update).
 *
 * Replica 3 is instructed to crash before processing its 2nd UPDATE message
 * (after_n=1 -> processes 1 normally, crashes before the next).
 *
 * First write:  all 5 replicas send ACK -> coordinator commits normally.
 * Second write: replica 3 crashes before ACKing -> coordinator reaches quorum
 *               from replicas 0,1,2,4 (4 ACKs >= Q=3) -> write still commits.
 *
 * Demonstrates: 2PC tolerates missing ACKs as long as quorum is reached.
 */
class CrashOnUpdate {

	private static final int N_NODES = 5;
	private static final int COORD = 0;

	@Test
	void crashOnUpdate() throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("crashOnUpdate", N_NODES, COORD);

		sys.actors.get(3).tell(new Crash(Crash.Type.Update, 1), Actor.noSender());

		TestKit probe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(1)), probe.getRef()),
				"client");

		// 1st write: all replicas participate
		client.tell(new AbstractClient.WriteRequest(5, 11), Actor.noSender());
		WriteResult firstWrite = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)*2), WriteResult.class);
		assertEquals(true, firstWrite.success, "First write must succeed: all replicas are still alive");

		// 2nd write: replica 3 crashes on UPDATE before ACKing
		client.tell(new AbstractClient.WriteRequest(5, 22), Actor.noSender());
		WriteResult secondWrite = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), WriteResult.class);
		assertEquals(true, secondWrite.success,
				"Second write must still succeed: quorum (3) is met by replicas 0,1,2,4");

		Thread.sleep(TestsCommons.getBaseMaxUpdateDelay(sys));

		client.tell(new AbstractClient.ReadRequest(5), Actor.noSender());
		ReadResult readResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)), ReadResult.class);
		assertEquals(22, readResult.value, "Read must return the value committed by the second write");

		sys.system.terminate();
	}
}
