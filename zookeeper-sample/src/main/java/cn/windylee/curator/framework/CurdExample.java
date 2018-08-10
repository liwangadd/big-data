package cn.windylee.curator.framework;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CurdExample {

    private static final Logger log = LoggerFactory.getLogger(CurdExample.class);

    public static void create(CuratorFramework client, String path, byte[] payload) throws Exception {
        client.create().creatingParentsIfNeeded().forPath(path, payload);
    }

    public static void createEphemeral(CuratorFramework client, String path, byte[] payload) throws Exception {
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
    }

    public static void createEphemeralSequential(CuratorFramework client, String path, byte[] payload) throws Exception {
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, payload);
    }

    public static void setData(CuratorFramework client, String path, byte[] payload) throws Exception {
        client.setData().forPath(path, payload);
    }

    public static void setDataAsync(CuratorFramework client, String path, byte[] payload) throws Exception {
        CuratorListener listener = (client1, curatorEvent) -> log.info("path: {}\t data: {}", curatorEvent.getPath(), curatorEvent.getData());
        client.getCuratorListenable().addListener(listener);
        client.setData().inBackground().forPath(path, payload);
    }

    public static void setDataAsyncWithCallback(CuratorFramework client, BackgroundCallback callback, String path, byte[] payload) throws Exception {
        client.setData().inBackground(callback).forPath(path, payload);
    }

    public static void delete(CuratorFramework client, String path) throws Exception {
        client.delete().forPath(path);
    }

    public static void guaranteedDelete(CuratorFramework client, String path) throws Exception {
        client.delete().guaranteed().forPath(path);
    }

    public static List<String> watchedGetChildren(CuratorFramework client, String path) throws Exception {
        return client.getChildren().watched().forPath(path);
    }

    public static List<String> watchedGetChildren(CuratorFramework client, String path, Watcher watcher) throws Exception {
        return client.getChildren().usingWatcher(watcher).forPath(path);
    }

    public static void main(String[] args) throws Exception {
        CuratorFramework client = CreateClient.createSimple("127.0.0.1:2181");

        client.start();
        create(client, "/windylee/persist", "persist".getBytes());
        create(client, "/windylee/persist_for_delete", "node for delete".getBytes());
        create(client, "/windylee/persist_for_guarantee_delete", "guarantee delete".getBytes());

        createEphemeral(client, "/windylee/ephemeral", "ephemeral".getBytes());
        createEphemeralSequential(client, "/windylee/ephemeral_sequential", "ephemeral sequential".getBytes());
        setData(client, "/windylee/persist", "persist new".getBytes());
        setDataAsync(client, "/windylee/persist", "persist async new".getBytes());
        setDataAsyncWithCallback(client, (curatorFramework, curatorEvent) -> log.info("path: {}\t data: {}", curatorEvent.getPath(), curatorEvent.getData()), "/windylee/persist", "persist with callback".getBytes());
        List<String> children = watchedGetChildren(client, "/windylee");
        for (String child : children) {
            log.info("child- " + child);
        }
        delete(client, "/windylee/persist_for_delete");
        watchedGetChildren(client, "/windylee", watchedEvent -> log.info("path: {}\tstate:{}\ttype:{}", watchedEvent.getPath(), watchedEvent.getState(), watchedEvent.getType()));
        guaranteedDelete(client, "/windylee/persist_for_guarantee_delete");
    }

}
