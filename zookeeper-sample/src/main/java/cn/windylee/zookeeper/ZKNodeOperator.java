package cn.windylee.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

public class ZKNodeOperator implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(ZKNodeOperator.class);

    private ZooKeeper zooKeeper = null;

    private static final String ZK_SERVER_PATH = "127.0.0.1:2181";
    private static final int TIMEOUT = 5000;

    public ZKNodeOperator(String connectString) {
        try {
            zooKeeper = new ZooKeeper(connectString, TIMEOUT, this);
        } catch (IOException e) {
            e.printStackTrace();
            if (zooKeeper != null) {
                try {
                    zooKeeper.close();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        ZKNodeOperator zkOperator = new ZKNodeOperator(ZK_SERVER_PATH);
//        zkOperator.createNode("/testnode", "testnode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
//        zkOperator.createNodeAsync("/testnode/xyz", "testnode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
//        zkOperator.setData("/testnode", "123".getBytes(), 0);
//        zkOperator.deleteNode("/test-delete-node", 0);
        zkOperator.deleteNodeAsync("/test-delete-node", 0);
        zkOperator.close();
    }

    //    同步创建节点
    private void createNode(String path, byte[] data, ArrayList<ACL> acls) {
        String result;
        try {
            /**
             * 同步创建节点，不支持子节点的递归创建
             * 参数：
             * path：创建的节点路径
             * data：存储的数据的byte[]
             * acl：控制权限策略
             *      Ids.OPEN_ACL_UNSAFE --> world:anyone:cdrwa
             *      Ids.CREATE_ALL_ACL --> auth:user:password:cdrwa
             * createMode：节点类型
             *      PERSISTENT：持久节点
             *      PERSISTENT_SEQUENTIAL：持久顺序节点
             *      EPHEMERAL：临时节点
             *      EPHEMERAL_SEQUENTIAL：临时顺序节点
             */
            result = zooKeeper.create(path, data, acls, CreateMode.PERSISTENT);
            log.warn("创建节点: \t" + result + "\t成功...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    异步创建节点
    private void createNodeAsync(String path, byte[] data, ArrayList<ACL> acls) {
        /**
         * 异步创建节点，需要指定回调类
         * 参数：
         * cb：回调类
         * ctx：该参数会传递给回调函数
         */
        zooKeeper.create(path, data, acls, CreateMode.EPHEMERAL, new CreateCallBack(), "createNode");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //    同步修改数据
    private void setData(String path, byte[] data, int version) {
        Stat stat;
        try {
            /**
             * 参数：
             * path：节点路径
             * data：数据
             * version：数据版本号
             */
            stat = zooKeeper.setData(path, data, version);
            log.warn(String.valueOf(stat.getVersion()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    同步删除节点
    private void deleteNode(String path, int version) {
        this.createNode(path, "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        try {
            /**
             * 参数
             * path：节点路径
             * version：数据版本号
             */
            zooKeeper.delete(path, 0);
            log.warn("删除节点: \t{}\t成功...", path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    异步删除节点
    private void deleteNodeAsync(String path, int version) {
        this.createNode(path, "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        zooKeeper.delete(path, 0, new DeleteCallBack(), "delete success");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

    }

    /**
     * 异步创建节点回调类
     */
    public static class CreateCallBack implements AsyncCallback.StringCallback {

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            log.warn("创建节点: " + path);
            log.warn("rc: {} path: {} ctx: {} name: {}", rc, path, ctx, name);
            log.warn((String) ctx);

        }
    }

    /**
     * 异步删除节点回调类
     */
    private static class DeleteCallBack implements AsyncCallback.VoidCallback {
        @Override
        public void processResult(int i, String s, Object o) {
            log.warn("s:\t{}, o:\t{}", s, o);
        }
    }
}
