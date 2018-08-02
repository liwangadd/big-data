package cn.windylee.zookeeper;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZKGetChildrenList implements Watcher {

    private ZooKeeper zooKeeper = null;

    private static final Logger log = LoggerFactory.getLogger(ZKGetChildrenList.class);
    private static final String ZK_SERVER_URL = "127.0.0.1:2181";
    private static final int TIMEOUT = 5000;
    private CountDownLatch countDownLatch;

    public ZKGetChildrenList(String connectString) {
        try {
            this.zooKeeper = new ZooKeeper(connectString, TIMEOUT, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            log.warn("NodeChildrenChanged");
            try {
                List<String> children = this.zooKeeper.getChildren(watchedEvent.getPath(), false);
                for (String child : children) {
                    log.warn(child);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        countDownLatch.countDown();
    }

    public void getChildren(String path) {
        /**
         * 参数：
         * path：父节点路径
         * watch：true/false, 是否注册监听事件
         */
        this.zooKeeper.getChildren(path, true, new Children2CallBack(), "list success");
    }

    public static void main(String[] args) throws InterruptedException {
        ZKGetChildrenList zk = new ZKGetChildrenList(ZK_SERVER_URL);
        zk.countDownLatch = new CountDownLatch(1);
        zk.getChildren("/imooc");
        zk.countDownLatch.await();
        zk.close();
    }

    private class Children2CallBack implements AsyncCallback.Children2Callback {
        @Override
        public void processResult(int i, String path, Object ctx, List<String> list, Stat stat) {
            for (String child : list) {
                log.warn(child);
            }
            log.warn("ChildrenCallback: {}", path);
            log.warn("ctx: {}", ctx);
            log.warn("stat: {}", stat.toString());
        }
    }

    public void close() {
        try {
            this.zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
