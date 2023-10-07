///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS info.picocli:picocli:4.6.3
//DEPS org.apache.curator:curator-recipes:5.5.0
//DEPS org.projectlombok:lombok:1.18.30
//DEPS org.slf4j:slf4j-nop:2.0.9


import lombok.AllArgsConstructor;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.RetryNTimes;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Command(name = "ZookeeperLeader", mixinStandardHelpOptions = true, version = "ZookeeperLeader 0.1",
        description = "ZookeeperLeader made with jbang")
class ZookeeperLeader implements Callable<Integer> {

    @Option(names = { "-p", "--election-path" }, description = "Path on zookeeper for the election")
    private String electionPath = "/election";

    @Option(names = { "-n", "--node-name" }, description = "Name of this node", required = true)
    private String nodeName;

    @Option(names = { "-d", "--shutdown-delay" },
            description = "Seconds to delay shutdown of system after it became the leader")
    private int shutdownDelay = 5;

    @Option(names = { "-e", "--exit" }, description = "Exit the leader without notifying Zookeeper")
    private boolean exitWithoutNotification;

    @Option(names = { "-c", "--connection" }, description = "The connection string for Zookeeper")
    private String connectionString = "localhost:2181";

    public static void main(String... args) {
        new CommandLine(new ZookeeperLeader()).execute(args);
    }

    @Override
    public Integer call() throws Exception {
        final CuratorFramework client = createClient();
        final LeaderLatch leaderLatch = new LeaderLatch( client, electionPath, nodeName );
        final Listener listener = new Listener(leaderLatch);
        leaderLatch.addListener(listener);
        leaderLatch.start();
        while (true) {
            System.out.println(nodeName + ": sleeping waiting on election");
            TimeUnit.SECONDS.sleep( 10 );
        }
    }

    private CuratorFramework createClient() throws InterruptedException {
        final int sleepMsBetweenRetries = 100;
        final int maxRetries = 3;
        final RetryPolicy retryPolicy = new RetryNTimes(maxRetries, sleepMsBetweenRetries);
        final CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString,
                retryPolicy);
        client.start();
        System.out.println(nodeName + ": trying to connect to Zookeeper in " + connectionString);
        client.blockUntilConnected(3, TimeUnit.SECONDS);
        if (client.getZookeeperClient().isConnected()) {
            System.out.println(nodeName + ": Connected to Zookeeper");
        } else {
            throw new IllegalStateException("Could not connect to Zookeeper in " + connectionString);
        }

        return client;
    }

    @AllArgsConstructor
    private class Listener implements LeaderLatchListener {
        private final LeaderLatch leaderLatch;

        @Override
        public void isLeader() {
            System.out.println(nodeName + ": is leader!");

            new Thread(this::uncheckedSleepAndExit).start();
        }

        @Override
        public void notLeader() {
            System.out.println(nodeName + ": is not leader!");
        }

        private void uncheckedSleepAndExit() {
            try {
                sleepAndExit();
            } catch (Exception e) {
                throw new IllegalStateException("Could not exit safely!", e);
            }
        }

        private void sleepAndExit() throws InterruptedException, IOException {
            try {
                TimeUnit.SECONDS.sleep( shutdownDelay );
            } finally {
                if (!exitWithoutNotification) {
                    System.out.println(nodeName + ": removing ourselves from the election!");
                    leaderLatch.close();
                }
            }
            System.out.println(nodeName + ": exiting!");
            System.exit( 0 );
        }
    }
}
