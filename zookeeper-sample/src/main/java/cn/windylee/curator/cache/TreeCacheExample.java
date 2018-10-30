package cn.windylee.curator.cache;

import cn.windylee.curator.framework.CreateClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TreeCacheExample {
    public static void main(String[] args) throws Exception {
        CuratorFramework client = CreateClient.createSimple("127.0.0.1:2181");
        client.getUnhandledErrorListenable().addListener((message, e) -> {
            System.err.println("error=" + message);
            e.printStackTrace();
        });
        client.getConnectionStateListenable().addListener((c, newState) -> {
            System.out.println("state=" + newState);
        });
        client.start();

        TreeCache cache = TreeCache.newBuilder(client, "/").setCacheData(false).build();
        cache.getListenable().addListener((c, event) -> {
            if (event.getData() != null) {
                System.out.println("type=" + event.getType() + " path=" + event.getData().getPath());
            } else {
                System.out.println("type=" + event.getType());
            }
        });
        cache.start();

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        in.readLine();
    }
}
