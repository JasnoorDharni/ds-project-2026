package it.unitn.ds.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.ElectionStarted;
import it.unitn.ds.AbstractReplica.UpdateApplied;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteResult;

public class APICompliance {

	/**
	 * For this test to work you need to call:
	 * 1) Use {@code AbstractClient.createBaseReceiveBuilder} instead of {@code AbstractActor.receiveBuilder}.
	 * 2) Properly implement the abstract method {@code AbstractReplica.initSystem(AbstractReplica.InitSystem)}
	 * 3) Invoke {@code AbstractClient.callbackOnWriteResult} whenever the system replies to a client write request.
	 * 4) Invoke {@code AbstractClient.callbackOnReadResult} whenever the system replies to a client read request.
	 * IMPORTANT: Failing to call these methods will make the test fail (even if the system works properly)
	 * 
	 * @param coordinator
	 * @param n_nodes
	 * @throws InterruptedException
	 */
	@ParameterizedTest(name = "client writes, waits and reads => coordinator {0}, nodes {1}")
	@CsvSource({
			"0,7",
			"0,22",
			"1,7",
			"1,22",
	})
	void oneClientWriteWaitRead(int coordinator, int n_nodes) throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("oneClientWriteWaitRead_" + coordinator, n_nodes,
				coordinator);
		TestKit probe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.ofNullable(sys.actors.get(0)), probe.getRef()),
				"client1");

		client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
				Actor.noSender());
		probe.expectMsgEquals(Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)),
				new WriteResult(true, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, 0));

		Thread.sleep(TestsCommons.getBaseMaxUpdateDelay(sys));

		client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
		ReadResult response = probe.expectMsgClass(Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)),
				ReadResult.class);
		assertEquals(TestsCommons.TEST_VALUE, response.value);

		sys.system.terminate();
	}

	/**
	 * For this test to work you need to call:
	 * 1) Use {@code AbstractReplica.createBaseReceiveBuilder} instead of {@code AbstractActor.receiveBuilder}.
	 * 2) The crash implementation (of `AbstractReplica.crash(AbstractReplica.Crash)`) must happen accordingly to {@code AbstractReplica.Crash} semantic.
	 * IMPORTANT: Failing to call this method will make the test fail (even if the system works properly)
	 */
	@Test
	void replicasCrashNow() {
		int N_NODES = 10;
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("replicasCrashNow", N_NODES, 0);

		for (int i = 0; i < sys.getNNodes(); i++) {
			sys.actors.get(i).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0), Actor.noSender());
			sys.probes.get(i).expectMsgEquals(Duration.ofMillis(200),
					new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0));
		}

		sys.system.terminate();
	}

	@ParameterizedTest(name = "replica crashes and client tries a requests to crashed replica => coordinator {0}")
	@CsvSource({
			"0",
			"1",
	})
	void crashReplicaAndTryRequests(int coordinator) {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("crashReplicaAndTryRequests_" + coordinator, 2,
				coordinator);

		TestKit client_probe = new TestKit(sys.system);
		ActorRef replica = sys.actors.get(0);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.ofNullable(replica), client_probe.getRef()),
				"client");

		replica.tell(new Crash(Crash.Type.Now, 0), Actor.noSender());

		client.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
		client_probe.expectMsgEquals(Duration.ofMillis((int) ((double) sys.client_read_timeout * 1.5)),
				new AbstractClient.ReadTimeout(client, replica, TestsCommons.TEST_INDEX));

		client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
				Actor.noSender());
		client_probe.expectMsgEquals(Duration.ofMillis((int) ((double) sys.client_write_timeout * 1.5)),
				new AbstractClient.WriteTimeout(client, replica, TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE));

		sys.system.terminate();
	}

	// =========================================================================
	// callbackOnUpdateApplied
	// =========================================================================

	/**
	 * For this test to work you need to:
	 * 1) Invoke {@code AbstractReplica.callbackOnUpdateApplied(index, value)} on
	 * EVERY replica immediately after it writes a new value into its local
	 * {@code positions[]} array in response to a WriteOK message.
	 * 2) The callback must be called with the correct {@code index} and {@code value} that were actually written.
	 * IMPORTANT: Failing to call this method will make the test fail (even if the system works properly).
	 */
	@ParameterizedTest(name = "callbackOnUpdateApplied is invoked on all replicas => coordinator {0}, nodes {1}")
	@CsvSource({
			"0,5",
			"0,7",
			"1,5",
			"1,7",
	})
	void callbackOnUpdateAppliedInvokedOnAllReplicas(int coordinator, int n_nodes) throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem(
				"callbackUpdateApplied_" + coordinator + "_" + n_nodes, n_nodes, coordinator);

		// Issue a write from a non-coordinator replica
		int target = (coordinator + 1) % n_nodes;
		TestKit writeProbe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(target)), writeProbe.getRef()),
				"client");

		client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
				Actor.noSender());

		// Wait until write is committed
		writeProbe.expectMsgClass(Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), WriteResult.class);

		// Every replica's probe must have received exactly one UpdateApplied
		// with the correct index and value
		long window = TestsCommons.getMaxUpdateDelay(sys);
		for (int i = 0; i < n_nodes; i++) {
			UpdateApplied ua = sys.probes.get(i).expectMsgClass(
					Duration.ofMillis(window),
					UpdateApplied.class);
			assertEquals(i, ua.replicaId,
					"UpdateApplied.replicaId must match the id of the reporting replica");
			assertEquals(TestsCommons.TEST_INDEX, ua.index,
					"UpdateApplied.index must match the written index");
			assertEquals(TestsCommons.TEST_VALUE, ua.value,
					"UpdateApplied.value must match the written value");
		}

		sys.system.terminate();
	}

	/**
	 * For this test to work you need to:
	 * 1) Invoke {@code callbackOnUpdateApplied} once per write, not more.
	 * Calling it multiple times for the same update would cause this test to fail.
	 * 2) The callback must carry the correct final value of each write in sequence.
	 */
	@ParameterizedTest(name = "callbackOnUpdateApplied called once per write, correct value => coordinator {0}, nodes {1}")
	@CsvSource({
			"0,5",
			"1,5",
	})
	void callbackOnUpdateAppliedOncePerWrite(int coordinator, int n_nodes) throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem(
				"callbackUpdateAppliedOnce_" + coordinator + "_" + n_nodes, n_nodes, coordinator);

		final int NUM_WRITES = 3;
		int target = (coordinator + 1) % n_nodes;
		TestKit writeProbe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(target)), writeProbe.getRef()),
				"client");

		for (int v = 1; v <= NUM_WRITES; v++) {
			client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, v * 10), Actor.noSender());
			// Wait for each write to fully commit before sending the next,
			// so probes receive events in a predictable order
			writeProbe.expectMsgClass(
					Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), WriteResult.class);
		}

		// Each replica must have received exactly NUM_WRITES UpdateApplied events,
		// one per write, with values 10, 20, 30 in that order.
		long window = TestsCommons.getMaxUpdateDelay(sys);
		for (int i = 0; i < n_nodes; i++) {
			for (int v = 1; v <= NUM_WRITES; v++) {
				UpdateApplied ua = sys.probes.get(i).expectMsgClass(
						Duration.ofMillis(window), UpdateApplied.class);
				assertEquals(TestsCommons.TEST_INDEX, ua.index,
						"UpdateApplied.index must be correct for write #" + v);
				assertEquals(v * 10, ua.value,
						"UpdateApplied.value must match write #" + v + " value on replica " + i);
			}
			// No extra UpdateApplied should follow
			sys.probes.get(i).expectNoMessage(Duration.ofMillis(200));
		}

		sys.system.terminate();
	}

	// =========================================================================
	// callbackOnElectionStarted
	// =========================================================================

	/**
	 * For this test to work you need to:
	 * 1) Invoke {@code AbstractReplica.callbackOnElectionStarted(crashedCoordinatorId)}
	 * exactly once on each replica that initiates or joins a coordinator election,
	 * when it sends its FIRST Election message for a given crashed coordinator.
	 * 2) The {@code crashedCoordinatorId} argument must be the id of the coordinator whose crash triggered the election.
	 * IMPORTANT: Failing to call this method will make the test fail (even if the system works properly).
	 */
	@ParameterizedTest(name = "callbackOnElectionStarted is invoked with correct coordinator id => coordinator {0}, nodes {1}")
	@CsvSource({
			"0,5",
			"0,7",
			"1,5",
			"1,7",
	})
	void callbackOnElectionStartedInvokedCorrectly(int coordinator, int n_nodes) throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem(
				"callbackElectionStarted_" + coordinator + "_" + n_nodes, n_nodes, coordinator);

		// Crash the coordinator immediately
		sys.actors.get(coordinator).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.probes.get(coordinator).expectMsgClass(Duration.ofMillis(300), Crash.class);

		// At least a quorum of non-coordinator replicas must fire ElectionStarted
		long window = TestsCommons.getElectionMaxDelay(sys);
		int quorum = (n_nodes / 2) + 1;
		List<ElectionStarted> received = new ArrayList<>();
		for (int i = 0; i < n_nodes; i++) {
			if (i == coordinator)
				continue;
			try {
				ElectionStarted es = sys.probes.get(i).expectMsgClass(
						Duration.ofMillis(window), ElectionStarted.class);
				received.add(es);
			} catch (AssertionError ignored) {
				// Not every replica needs to independently initiate — ring propagation
				// means some may only forward. At minimum quorum-1 must fire.
			}
		}

		assertTrue(received.size() >= quorum - 1,
				"At least a quorum of non-coordinator replicas must invoke callbackOnElectionStarted. "
						+ String.format("Received %d ElectionStarted but quorum is %d", received.size(), quorum - 1));

		for (ElectionStarted es : received) {
			assertEquals(coordinator, es.crashedCoordinatorId,
					"ElectionStarted.crashedCoordinatorId must equal the id of the crashed coordinator");
			assertNotEquals(coordinator, es.replicaId,
					"ElectionStarted.replicaId must not be the crashed coordinator");
		}

		sys.system.terminate();
	}

	/**
	 * For this test to work you need to:
	 * 1) Invoke {@code callbackOnElectionStarted} at most ONCE per replica per
	 * election. Calling it multiple times for the same crashed coordinator
	 * on the same replica will make this test fail.
	 */
	@Test
	void callbackOnElectionStartedCalledAtMostOncePerReplica() throws InterruptedException {
		final int N = 7;
		final int COORD = 0;
		final TestsSystemWrapper sys = TestsCommons.createTestSystem(
				"callbackElectionStartedOnce", N, COORD);

		sys.actors.get(COORD).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.probes.get(COORD).expectMsgClass(Duration.ofMillis(300), Crash.class);

		long window = TestsCommons.getElectionMaxDelay(sys);

		// Drain all messages from each non-coordinator probe within the election window
		for (int i = 1; i < N; i++) {
			List<Object> allMsgs = new ArrayList<>();
			// Collect everything within the window
			try {
				while (true) {
					Object msg = sys.probes.get(i).expectMsgAnyClassOf(
							Duration.ofMillis(window),
							ElectionStarted.class, CoordinatorElected.class,
							AbstractReplica.Crash.class);
					allMsgs.add(msg);
					// Shorten window after first message to drain quickly
					window = 200;
				}
			} catch (AssertionError ignored) {
			}

			long electionStartedCount = allMsgs.stream()
					.filter(m -> m instanceof ElectionStarted)
					.count();
			assertTrue(electionStartedCount <= 1,
					"Replica " + i + " must invoke callbackOnElectionStarted at most once per election, " +
							"but it was called " + electionStartedCount + " times");
		}

		sys.system.terminate();
	}

	// =========================================================================
	// callbackOnCoordinatorElected
	// =========================================================================

	/**
	 * For this test to work you need to:
	 * 1) Invoke
	 * {@code AbstractReplica.callbackOnCoordinatorElected(newCoordinatorId)}
	 * on every replica that learns the outcome of a coordinator election:
	 * - The winning replica must call it when it decides it has won.
	 * - Every other replica must call it when it processes the Synchronization
	 * message from the new coordinator.
	 * 2) The {@code newCoordinatorId} argument must be the id of the replica
	 * that won the election (i.e. has the most recent update).
	 * 3) ALL replicas that call this callback must agree on the SAME
	 * {@code newCoordinatorId}.
	 * IMPORTANT: Failing to call this method will make the test fail
	 * (even if the system works properly).
	 */
	@ParameterizedTest(name = "callbackOnCoordinatorElected: all replicas agree on same new coordinator => coordinator {0}, nodes {1}")
	@CsvSource({
			"0,5",
			"0,7",
			"1,5",
			"1,7",
	})
	void callbackOnCoordinatorElectedAllAgree(int coordinator, int n_nodes) throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem(
				"callbackCoordElected_" + coordinator + "_" + n_nodes, n_nodes, coordinator);

		// Crash coordinator
		sys.actors.get(coordinator).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.probes.get(coordinator).expectMsgClass(Duration.ofMillis(300), Crash.class);

		long window = TestsCommons.getElectionMaxDelay(sys);
		int quorum = (n_nodes / 2) + 1;

		List<CoordinatorElected> received = new ArrayList<>();
		for (int i = 0; i < n_nodes; i++) {
			if (i == coordinator)
				continue;
			try {
				CoordinatorElected ce = (CoordinatorElected) sys.probes.get(i).fishForMessage(
						Duration.ofMillis(window),
						"",
						msg -> msg instanceof CoordinatorElected);
				received.add(ce);
			} catch (AssertionError ignored) {
			}
		}

		assertTrue(received.size() >= quorum - 1,
				"At least a quorum of surviving replicas must invoke callbackOnCoordinatorElected. " + String
						.format("Received %d CoordinatorElected but quorum is %d", received.size(), quorum - 1));

		// All must agree on the same new coordinator
		int elected = received.get(0).newCoordinatorId;
		assertNotEquals(coordinator, elected,
				"The new coordinator must not be the crashed replica");
		for (CoordinatorElected ce : received) {
			assertEquals(elected, ce.newCoordinatorId,
					"All replicas must report the SAME new coordinator id");
			assertNotEquals(coordinator, ce.replicaId,
					"CoordinatorElected.replicaId must not be the crashed coordinator");
		}

		sys.system.terminate();
	}

	/**
	 * For this test to work you need to:
	 * 1) Invoke {@code callbackOnCoordinatorElected} with the correct id.
	 *    Specifically, after a write is committed before the crash, the elected
	 *    coordinator must be the replica with the most up-to-date history,
	 *    so the {@code newCoordinatorId} must reflect the protocol's choice.
	 * 2) The replica that reports itself as the new coordinator
	 *    (i.e. {@code CoordinatorElected.replicaId == CoordinatorElected.newCoordinatorId})
	 *    must also invoke the callback.
	 */
	@ParameterizedTest(name = "callbackOnCoordinatorElected: new coordinator also calls the callback => coordinator {0}, nodes {1}")
	@CsvSource({
			"0,5",
			"0,7",
			"1,5",
	})
	void callbackOnCoordinatorElectedNewCoordAlsoCalls(int coordinator, int n_nodes) throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem(
				"callbackCoordElectedSelf_" + coordinator + "_" + n_nodes, n_nodes, coordinator);

		// Commit a write first so there is a non-trivial update history
		int target = (coordinator + 1) % n_nodes;
		TestKit wProbe = new TestKit(sys.system);
		ActorRef wClient = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(target)), wProbe.getRef()),
				"wClient");
		wClient.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE),
				Actor.noSender());
		wProbe.expectMsgClass(Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), WriteResult.class);

		// Crash coordinator
		sys.actors.get(coordinator).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.probes.get(coordinator).fishForMessage(Duration.ofMillis(300), "", msg -> msg instanceof Crash);

		long window = TestsCommons.getElectionMaxDelay(sys);

		// Collect all CoordinatorElected events, skipping any other messages
		// (e.g. UpdateApplied) that may arrive before the election completes.
		List<CoordinatorElected> received = new ArrayList<>();
		for (int i = 0; i < n_nodes; i++) {
			if (i == coordinator)
				continue;
			try {
				CoordinatorElected ce = (CoordinatorElected) sys.probes.get(i).fishForMessage(
						Duration.ofMillis(window),
						"",
						msg -> msg instanceof CoordinatorElected);
				received.add(ce);
			} catch (AssertionError ignored) {
				// Probe timed out without seeing a CoordinatorElected — tolerated,
				// as long as at least one replica fires the callback.
			}
		}
		assertTrue(!received.isEmpty(), "At least one replica must report CoordinatorElected");

		int elected = received.get(0).newCoordinatorId;

		// The elected replica itself must have fired the callback
		// (i.e. one of the received events must have replicaId == newCoordinatorId)
		boolean newCoordFiredCallback = received.stream()
				.anyMatch(ce -> ce.replicaId == elected && ce.newCoordinatorId == elected);
		assertTrue(newCoordFiredCallback,
				"The newly elected coordinator (id=" + elected + ") must also invoke " +
						"callbackOnCoordinatorElected with its own id");

		sys.system.terminate();
	}
}
