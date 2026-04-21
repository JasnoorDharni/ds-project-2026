package it.unitn.ds.base;

import akka.testkit.javadsl.TestKit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import akka.actor.Actor;
import akka.actor.ActorRef;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import it.unitn.ds.AbstractReplica.Crash;

/**
 * IMPORTANT: Before running these tests, make sure you comply with the API and
 * callback rules.
 * Check out the java doc and tests in `APICompliance.java`.
 */
class WithCrashes {

	@ParameterizedTest(name = "some non-coordinator replicas crash, client writes, waits and reads => coordinator {0}, nodes {1}")
	@CsvSource({
			"7",
			"22",
	})
	void nonCoordinatorsCrashClientWritesWaitsReads(int n_nodes) throws InterruptedException {
		final int COORDINATOR_ID = 0;
		final TestsSystemWrapper sys = TestsCommons
				.createTestSystem("nonCoordinatorsCrashClientWritesWaitsReads_" + COORDINATOR_ID, n_nodes,
						COORDINATOR_ID);
		TestKit probe = new TestKit(sys.system);
		int targetReplicaID = n_nodes - 1;
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.ofNullable(sys.actors.get(targetReplicaID)), probe.getRef()),
				"client");

		for (int i = 0; i < n_nodes / 2 - 2; i++) {
			sys.actors.get(i + 1).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		}

		client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
				Actor.noSender());
		WriteResult wr = (WriteResult) probe.fishForMessage(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
				"WriteResult",
				msg -> msg instanceof WriteResult);
		assertEquals(
				new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), wr);

		Thread.sleep(TestsCommons.getMaxUpdateDelay(sys));

		client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
		ReadResult rr = (ReadResult) probe.fishForMessage(
				Duration.ofMillis(sys.client_read_timeout),
				"ReadResult",
				msg -> msg instanceof ReadResult);
		assertEquals(
				new ReadResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), rr);

		sys.system.terminate();
	}

	@ParameterizedTest(name = "coordinator and other 2 replicas crash, client writes, waits and reads => coordinator {0}, nodes {1}")
	@CsvSource({
			"1,7",
			"0,22",
	})
	void coordinatorCrashClientWritesWaitsReads(int coordinator, int n_nodes) throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("coordinatorCrashClientWritesWaitsReads_" + coordinator, n_nodes, coordinator);
		TestKit probe = new TestKit(sys.system);
		int targetReplicaID = n_nodes - 1;
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.ofNullable(sys.actors.get(targetReplicaID)), probe.getRef()),
				"client");

		for (int i = 0; i < 3; i++) {
			sys.actors.get(i).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		}

		client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
				Actor.noSender());
		WriteResult wr = (WriteResult) probe.fishForMessage(
				Duration.ofMillis(TestsCommons.getElectionMaxDelay(sys) + TestsCommons.getMaxUpdateDelay(sys)),
				"WriteResult",
				msg -> msg instanceof WriteResult);
		assertEquals(
				new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), wr);

		client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
		ReadResult rr = (ReadResult) probe.fishForMessage(
				Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
				"ReadResult",
				msg -> msg instanceof ReadResult);
		assertEquals(
				new ReadResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, targetReplicaID), rr);

		sys.system.terminate();
	}
}