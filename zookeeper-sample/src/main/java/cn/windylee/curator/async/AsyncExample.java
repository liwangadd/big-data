package cn.windylee.curator.async;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncEventException;
import org.apache.curator.x.async.WatchMode;
import org.apache.zookeeper.WatchedEvent;

import java.util.concurrent.CompletionStage;

public class AsyncExample {

    public static AsyncCuratorFramework wrap(CuratorFramework client) {
        return AsyncCuratorFramework.wrap(client);
    }

    public static void create(CuratorFramework client, String path, byte[] payload) {
        AsyncCuratorFramework asyncClient = wrap(client);
        asyncClient.create().forPath(path, payload).whenComplete((name, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                System.out.println("Created node name is: " + name);
            }
        });
    }

    public static void createThenWatch(CuratorFramework client, String path) {
        AsyncCuratorFramework asyncClient = AsyncCuratorFramework.wrap(client);
        asyncClient.create().forPath(path).whenComplete((name, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                handleWatchedState(asyncClient.watched().checkExists().forPath(path).event());
            }
        });
    }

    public static void createThenWatchSimple(CuratorFramework client, String path) {
        AsyncCuratorFramework asyncClient = wrap(client);
        asyncClient.create().forPath(path).whenComplete((name, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                asyncClient.with(WatchMode.successOnly).watched().checkExists().forPath(path)
                        .event().thenAccept(event -> {
                    System.out.println(event.getType());
                    System.out.println(event);
                });
            }
        });
    }

    private static void handleWatchedState(CompletionStage<WatchedEvent> watchedState) {
        watchedState.thenAccept(event -> {
            System.out.println(event.getType());
            System.out.println(event);
        });
        watchedState.exceptionally(exception -> {
            AsyncEventException asyncEx = (AsyncEventException) exception;
            asyncEx.printStackTrace();
            handleWatchedState(asyncEx.reset());
            return null;
        });
    }

}
