package cn.windylee.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZKNodeExist implements Watcher {

    private ZooKeeper zooKeeper = null;

    private static final Logger log = LoggerFactory.getLogger(ZKGetNodeData.class);
    private static final String ZK_SERVER_URL = "127.0.0.1:2181";
    private static final int TIMEOUT = 5000;
    private CountDownLatch countDownLatch;

    public ZKNodeExist(String connectString) {
        try {
            this.zooKeeper = new ZooKeeper(connectString, TIMEOUT, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void isNodeExits(String path, boolean isWatch) {
        try {
            Stat stat = this.zooKeeper.exists(path, isWatch);
            if (stat != null) {
                log.warn("查询的节点版本为：{}", stat.getVersion());
            } else {
                log.warn("该节点不存在");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeCreated) {
            log.warn("节点{}被创建", watchedEvent.getPath());
        }
        this.countDownLatch.countDown();
    }

    public static void main(String[] args) throws InterruptedException {
        ZKNodeExist zk = new ZKNodeExist(ZK_SERVER_URL);
        zk.countDownLatch = new CountDownLatch(1);
        zk.isNodeExits("/imooc-test", true);
        zk.countDownLatch.await();
    }
}
