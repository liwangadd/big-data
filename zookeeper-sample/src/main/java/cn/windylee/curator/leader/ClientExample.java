package cn.windylee.curator.leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientExample extends LeaderSelectorListenerAdapter implements Closeable {

    private final String name;
    private final LeaderSelector leaderSelector;
    private final AtomicInteger leaderCount = new AtomicInteger();

    private static final Logger log = LoggerFactory.getLogger(ClientExample.class);

    public ClientExample(CuratorFramework client, String path, String name) {
        this.name = name;
        leaderSelector = new LeaderSelector(client, path, this);
        leaderSelector.autoRequeue();
    }

    public void start() {
        leaderSelector.start();
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        final int waitSeconds = (int) (5 * Math.random() + 1);
        log.info(name + " is now the leader. Waiting " + waitSeconds + " seconds...");
        log.info(name + " has bean leader " + leaderCount.getAndIncrement() + " time(s) before");
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        } catch (InterruptedException e) {
            log.error(name + " was interrupted");
            Thread.currentThread().interrupt();
        } finally {
            log.info(name + " relinquishing leadership");
        }

    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }
}
