package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractReplica.InitSystem;

/**
 * NOTE: ALL TIMINGS ON THESE CLASS MAY BE CHANGED.
 * Make sure your implementation is robust and performant enough. 
 */
public class TestsCommons {

    public static final int TEST_INDEX = 0;
    public static final int TEST_VALUE = 10;
    public static final int TEST_COORDINATOR_BEAT_INTERVAL = AbstractReplica.COORDINATOR_BEAT_INTERVAL;

    public static final boolean DO_PRINTS = false;
    public static final boolean DO_DEBUG_PRINTS = false;

    public static class TestsSystemWrapper {
        public final ActorSystem system;
        public final Map<Integer, ActorRef> actors;
        public final Map<Integer, TestKit> probes;
        public final int min_latency;
        public final int max_latency;
        public final long client_read_timeout;
        public final long client_write_timeout;

        public TestsSystemWrapper(ActorSystem system, Map<Integer, ActorRef> actors, Map<Integer, TestKit> probes,
                int min_latency, int max_latency) {
            this.system = system;
            this.actors = actors;
            this.probes = probes;
            this.min_latency = min_latency;
            this.max_latency = max_latency;
            this.client_read_timeout = getClientReadTimeout(max_latency, actors.size());
            this.client_write_timeout = getClientWriteTimeout(max_latency, actors.size());
        }

        public TestsSystemWrapper(ActorSystem system, Map<Integer, ActorRef> actors, Map<Integer, TestKit> probes) {
            this.system = system;
            this.actors = actors;
            this.probes = probes;
            this.min_latency = AbstractReplica.MIN_LATENCY;
            this.max_latency = AbstractReplica.MAX_LATENCY;
            this.client_read_timeout = getClientReadTimeout(max_latency, actors.size());
            this.client_write_timeout = getClientWriteTimeout(max_latency, actors.size());
        }

        public int getNNodes() {
            return actors.size();
        }
    }

    public static TestsSystemWrapper createTestSystem(String name, int n_actors, int coordinator, int min_latency, int max_latency) {
        assert (coordinator < n_actors);
        final ActorSystem system = ActorSystem.create(name);

        if (DO_PRINTS) {
            Logger.setDestinationStdout();
            Logger.setDebugEnabled(DO_DEBUG_PRINTS);
        } else {
            Logger.disable();
        }

        Map<Integer, ActorRef> group = new HashMap<>();
        Map<Integer, TestKit> probes = new HashMap<>();
        for (int i = 0; i < n_actors; i++) {
            TestKit probe = new TestKit(system);
            probes.put(i, probe);
            group.put(i, system.actorOf(
                            Replica.propsWithListener(i, min_latency, max_latency, TEST_COORDINATOR_BEAT_INTERVAL,
                                    probe.getRef()),
                            "Replica_" + i)
            );
        }

        InitSystem initMsg = new InitSystem(group, coordinator);
        for (Map.Entry<Integer, ActorRef> entry : group.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }

        return new TestsSystemWrapper(system, group, probes);
    }

    public static TestsSystemWrapper createTestSystem(String name, int n_actors, int coordinator) {
        return createTestSystem(name, n_actors, coordinator, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY);
    }

    public static int getLatencyPlusEpsilon(TestsSystemWrapper sys) {
        // Account for the time it takes to iterate through peers in a loop
        return sys.max_latency * sys.getNNodes();
    }

    public static int getMaxUpdateDelay(TestsSystemWrapper sys) {
        // 2PC round = 4 hops (client→replica→coord, coord→all, all→coord, coord→all)
        // each hop = max_latency, plus scheduling jitter
        return 6 * sys.max_latency + sys.getNNodes() + 200;
    }

    public static int getBaseMaxUpdateDelay(TestsSystemWrapper sys) {
        return 6 * sys.max_latency + 200;
    }

    public static long getElectionMaxDelay(TestsSystemWrapper sys) {
        // Crash detection + the ring circulation (N hops)
        long detection = (long) (TEST_COORDINATOR_BEAT_INTERVAL * 3.0) + (sys.max_latency * sys.getNNodes() * 2);
        long ringHops = (long) sys.getNNodes() * sys.max_latency * 2;
        return (detection + ringHops) * 5;
    }

    public static long getClientReadTimeout(int max_lat, int n_nodes) {
        return max_lat * n_nodes * 8;
    }

    public static long getClientWriteTimeout(int max_lat, int n_nodes) {
        long detection = (long) (TEST_COORDINATOR_BEAT_INTERVAL * 3.0) + (max_lat * n_nodes * 2);
        long ringHops = (long) n_nodes * max_lat * 2;
        return getClientReadTimeout(max_lat, n_nodes) + (detection + ringHops) * 5;
    }
}
