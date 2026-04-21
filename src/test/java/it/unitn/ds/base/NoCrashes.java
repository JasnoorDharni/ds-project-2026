package it.unitn.ds.base;

import akka.testkit.javadsl.TestKit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import akka.actor.Actor;
import akka.actor.ActorRef;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

/**
 * IMPORTANT: Before running these tests, make sure you comply with the API and callback rules.
 * Check out the java doc and tests in `APICompliance.java`.
 */
class NoCrashes {

	@ParameterizedTest(name = "seqConsistency 1 write client ==> coordinator {0}, nodes {1}")
	@CsvSource({
		"0,7",
		"0,22",
		"1,7",
		"1,22",
	})
	void sequentialConsistencyOneWriteClient(int coordinator, int n_nodes) throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("sequentialConsistencyOneWriteClient_" + coordinator, n_nodes, coordinator);

		final int NUM_WRITES = 5;
		final int NUM_READS_PER_CLIENT = 15;
		final int READ_INTERVAL_MS = TestsCommons.getMaxUpdateDelay(sys) / 2;

		TestKit readProbe = new TestKit(sys.system);

		// Create write client with its own probe
		ActorRef writeClient = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout, Optional.ofNullable(sys.actors.get(0)), null),
				"clientWrite");

		// Create read clients with read probe
		List<ActorRef> readClients = new ArrayList<>();
		for (int i = 0; i < sys.actors.size(); i++) {
			ActorRef readClient = sys.system.actorOf(
					Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout, Optional.ofNullable(sys.actors.get(i)), readProbe.getRef()),
					"clientRead_replica_" + i);
			readClients.add(readClient);
		}

		// Start all read clients in parallel
		CountDownLatch startLatch = new CountDownLatch(1);
		List<Thread> readThreads = new ArrayList<>();

		for (ActorRef readClient : readClients) {
			Thread readThread = new Thread(() -> {
				try {
					startLatch.await();
					for (int i = 0; i < NUM_READS_PER_CLIENT; i++) {
						readClient.tell(new AbstractClient.ReadRequest(TestsCommons.TEST_INDEX), Actor.noSender());
						Thread.sleep(READ_INTERVAL_MS);
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			});
			readThread.start();
			readThreads.add(readThread);
		}

		// Send all writes
		for (int val = 0; val < NUM_WRITES; val++) {
			writeClient.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, val), Actor.noSender());
		}

		// Start all read requests simultaneously
		startLatch.countDown();

		// Wait for all read threads to complete
		for (Thread thread : readThreads) {
			thread.join();
		}

		// Collect and verify sequential consistency
		Map<Integer, List<Integer>> replicaValues = new HashMap<>();
		int totalExpectedReads = readClients.size() * NUM_READS_PER_CLIENT;

		for (int i = 0; i < totalExpectedReads; i++) {
			ReadResult result = readProbe.expectMsgClass(ReadResult.class);
			replicaValues.computeIfAbsent(result.fromReplica, k -> new ArrayList<>()).add(result.value);
		}

		sys.system.terminate();

		// Verify sequential consistency
		System.out.println("- TEST OUTCOME (coordinator: " + coordinator + ") -");
		for (Map.Entry<Integer, List<Integer>> entry : replicaValues.entrySet()) {
			int replicaId = entry.getKey();
			List<Integer> values = entry.getValue();

			System.out.println("Replica " + replicaId + " values: " + values);

			for (int i = 1; i < values.size(); i++) {
				assertTrue(
						values.get(i - 1) == null || values.get(i) >= values.get(i - 1),
						String.format(
								"Sequential consistency violated for replica %d: value at index %d (%d) is less than previous value (%d)",
								replicaId, i, values.get(i), values.get(i - 1)));
			}
		}
	}

}
