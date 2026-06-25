package it.unitn.ds.extra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.io.Tcp.Write;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;

class Election {
    /**
     * case where the elected winner crashes before broadcasting SYNC.
     *
     * as there are no prior messages we expect the replica with the highest ID to win the election
     */
	@ParameterizedTest
	@CsvSource({
			"0,5",
			"2,6",
			"3,5",
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

		Thread.sleep(TestsCommons.getElectionMaxDelay(sys)/2); 

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

    // quorum is 2 because there are 2 replicas, so the writes cannot reach quorum
	@ParameterizedTest
	@CsvSource({
			"0,2",
			"1,2",
	   })
	void lastReplicaStanding(int coord, int n_nodes) throws InterruptedException {
		final TestsSystemWrapper sys = TestsCommons.createTestSystem("crashOnElection", n_nodes, coord);


		sys.actors.get(0).tell(new Crash(Crash.Type.Now, 0), Actor.noSender());
		sys.probes.get(0).fishForMessage(Duration.ofMillis(300), "", msg -> msg instanceof Crash);


		TestKit probe = new TestKit(sys.system);
		ActorRef client = sys.system.actorOf(
				Client.propsWithListener(sys.client_read_timeout, sys.client_write_timeout,
						Optional.of(sys.actors.get(1)), probe.getRef()),
				"client");

		Thread.sleep(TestsCommons.getElectionMaxDelay(sys)/2); 

		client.tell(new AbstractClient.WriteRequest(0, 33), Actor.noSender());

		probe.expectNoMessage(Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)));

		sys.system.terminate();
	}

}
