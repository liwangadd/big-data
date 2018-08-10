package cn.windylee.curator.locking;

import java.util.concurrent.atomic.AtomicBoolean;

public class FakeLimitedResource {

    private final AtomicBoolean inUse = new AtomicBoolean(false);

    public void use() {
        if (!inUse.compareAndSet(false, true)) {
            throw new IllegalStateException("Needs to be used by one client at a time");
        }
        try {
            Thread.sleep((long) (Math.random() * 3));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            inUse.set(false);
        }
    }

}
