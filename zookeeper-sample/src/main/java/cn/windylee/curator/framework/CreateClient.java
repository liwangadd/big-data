package cn.windylee.curator.framework;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CreateClient {

    public static CuratorFramework createSimple(String connectionStr) {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.newClient(connectionStr, retryPolicy);
    }

    public static CuratorFramework createWithOptions(String connectionStr, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs) {
        return CuratorFrameworkFactory.builder()
                .connectString(connectionStr)
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                .build();
    }

}
