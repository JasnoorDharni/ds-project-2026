package it.unitn.ds.extra;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

class Election {

	// private static final int n_nodes = 5;
	// private static final int coord = 0;


    /**
     * crash that triggers ring election  (Crash.Type.Now on two replicas).
     *
     * The coordinator and replica 2 crash immediately. When the surviving replicas start the
     * ring election, they try to send the ELECTION message to replica 2 (ring order),
     * receive no ElectionAck, hit the ElectionAckTimeout, skip replica 2, and continue
     * the ring to the next live replica. Election completes; write + read succeed.
     */
	@ParameterizedTest
	@CsvSource({
			"0,5,1,6",
			"2,0,3,5",
			"3,4,1,5",
    })
	void crashTwoThenElection(int coord, int crash_node, int request_node, int n_nodes) throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("crashOnElection", n_nodes, coord);

		sys.actors.get(coord).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.actors.get(crash_node).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.probes.get(coord).fishForMessage(Duration.ofMillis(300), "", msg -> msg instanceof Crash);
		sys.probes.get(crash_node).fishForMessage(Duration.ofMillis(300), "", msg -> msg instanceof Crash);

        Thread.sleep(TestsCommons.getElectionMaxDelay(sys)); // is longer than necessary but just to use provided functions

        // set up client to send requests to `request_node`
		TestKit probe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(request_node)), probe.getRef()),
				"client");

        // write request
		client.tell(new AbstractClient.WriteRequest(0, 33), Actor.noSender());
		WriteResult writeResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), WriteResult.class);
		assertEquals(true, writeResult.success,
				"Write must succeed: the ring election skips the silent replica 2 and still elects a coordinator");

		Thread.sleep(TestsCommons.getBaseMaxUpdateDelay(sys));

        // read request
		client.tell(new AbstractClient.ReadRequest(0), Actor.noSender());
		ReadResult readResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)), ReadResult.class);
		assertEquals(33, readResult.value, "Read must return the value committed after the election");

		sys.system.terminate();
	}


    /**
     * case where the elected winner crashes before broadcasting SYNC.
     *
     * as there are no prior messages we expect the replica with the highest ID to win the election
     */
	@ParameterizedTest
	@CsvSource({
			"0,5",
			"2,15",
			"7,10",
	   })
	void crashElectedWinnerBeforeSync(int coord, int n_nodes) throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("crashOnElection", n_nodes, coord);


		sys.actors.get(coord).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.probes.get(coord).fishForMessage(Duration.ofMillis(300), "", msg -> msg instanceof Crash);
        // even just after handling 1 election message it can crash as that message will make the complete loop and lead to this replica winning
		sys.actors.get(n_nodes -1).tell(new Crash(Crash.Type.Election, 1), Actor.noSender());



		TestKit probe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(1)), probe.getRef()),
				"client");

		Thread.sleep(TestsCommons.getElectionMaxDelay(sys));

	       // write check
		client.tell(new AbstractClient.WriteRequest(0, 33), Actor.noSender());
		WriteResult writeResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)), WriteResult.class);
		assertEquals(true, writeResult.success,
				"Write must succeed");

		Thread.sleep(TestsCommons.getBaseMaxUpdateDelay(sys));

	       // read check
		client.tell(new AbstractClient.ReadRequest(0), Actor.noSender());
		ReadResult readResult = probe.expectMsgClass(
				Duration.ofMillis(TestsCommons.getLatencyPlusEpsilon(sys)), ReadResult.class);
		assertEquals(33, readResult.value, "Read must return the value committed after the election");

		sys.system.terminate();
	}
}
