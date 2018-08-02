package cn.windylee.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZKGetNodeData implements Watcher {

    private ZooKeeper zooKeeper = null;
    private static final Logger log = LoggerFactory.getLogger(ZKGetNodeData.class);
    private static final String ZK_SERVER_URL = "127.0.0.1:2181";
    private static final int TIMEOUT = 5000;
    private Stat stat = new Stat();
    private CountDownLatch countDownLatch;

    public ZKGetNodeData(String connectString) {
        try {
            this.zooKeeper = new ZooKeeper(connectString, TIMEOUT, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
                byte[] data = this.zooKeeper.getData("/imooc", false, this.stat);
                log.warn("改变后的值：{}", new String(data));
                log.warn("改变后的版本号：{}", this.stat.getVersion());
                this.countDownLatch.countDown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getNodeData(String path, boolean watch) {
        try {
            byte[] data = this.zooKeeper.getData(path, watch, this.stat);
            log.warn("当前节点内容：{}", new String(data));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ZKGetNodeData zk = new ZKGetNodeData(ZK_SERVER_URL);
        zk.countDownLatch = new CountDownLatch(1);
        zk.getNodeData("/imooc", true);
        zk.countDownLatch.await();
        zk.close();
    }
}
