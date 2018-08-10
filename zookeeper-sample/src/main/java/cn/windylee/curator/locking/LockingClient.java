package cn.windylee.curator.locking;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class LockingClient {

    private static final Logger log = LoggerFactory.getLogger(LockingClient.class);

    private final InterProcessLock lock;
    private final FakeLimitedResource resource;
    private final String clientName;

    public LockingClient(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
        this.resource = resource;
        this.clientName = clientName;
        lock = new InterProcessMutex(client, lockPath);
    }

    public void doWork(long time, TimeUnit unit) throws Exception {
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException("{} could not acquire the lock");
        }
        try {
            log.info("{} has the lock", clientName);
            resource.use();
        } finally {
            log.info("{} releasing the lock", clientName);
            lock.release();
        }
    }

}
