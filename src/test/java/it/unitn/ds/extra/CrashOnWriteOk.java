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
 * Scenario 5 — deferred crash on WRITEOK (Crash.Type.WriteOK).
 *
 * Replica 3 is instructed to crash before applying its 2nd WRITEOK
 * (after_n=1 -> applies 1 normally, crashes before the next).
 *
 * First write:  all replicas apply the commit (replica 3 applies its 1st WriteOK).
 * Second write: replica 3 crashes before applying -> its positions[] stays stale,
 *               but the commit is still valid (replicas 0,1,2,4 all applied it).
 *
 * Then the coordinator crashes -> ring election among 1,2,4 -> new coordinator
 * broadcasts SYNCHRONIZATION with the full committed history. This is the mechanism
 * by which a replica that missed a WRITEOK would catch up upon recovery.
 */
class CrashOnWriteOk {

	private static final int N_NODES = 5;
	private static final int COORD = 0;

	@Test
	void crashOnWriteOk() throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("crashOnWriteOk", N_NODES, COORD);

		sys.actors.get(3).tell(new Crash(Crash.Type.WriteOK, 1), Actor.noSender());

		TestKit probe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(1)), probe.getRef()),
				"client");

		// 1st write: replica 3 applies normally
		client.tell(new AbstractClient.WriteRequest(10, 55), Actor.noSender());
		WriteResult firstWrite = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)*2), WriteResult.class);
		assertEquals(true, firstWrite.success, "First write must succeed normally");

		// 2nd write: replica 3 crashes before applying the WriteOK
		client.tell(new AbstractClient.WriteRequest(10, 66), Actor.noSender());
		WriteResult secondWrite = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys)), WriteResult.class);
		assertEquals(true, secondWrite.success,
				"Second write must still succeed: replicas 0,1,2,4 all applied it");

		// Crash coordinator -> election; new coordinator sends SYNCHRONIZATION with committed history
		sys.actors.get(COORD).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.probes.get(COORD).fishForMessage(Duration.ofMillis(300), "", msg -> msg instanceof Crash);
		Thread.sleep(AbstractReplica.COORDINATOR_BEAT_INTERVAL * 3L);

		client.tell(new AbstractClient.WriteRequest(10, 77), Actor.noSender());
		WriteResult thirdWrite = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys)), WriteResult.class);
		assertEquals(true, thirdWrite.success, "Third write must succeed under the newly elected coordinator");

		Thread.sleep(TestsCommons.getBaseMaxUpdateDelay(sys));

		client.tell(new AbstractClient.ReadRequest(10), Actor.noSender());
		ReadResult readResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys)), ReadResult.class);
		assertEquals(77, readResult.value, "Read must return the most recently committed value");

		sys.system.terminate();
	}
}
