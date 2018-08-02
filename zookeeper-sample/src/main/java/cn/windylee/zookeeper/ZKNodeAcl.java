package cn.windylee.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class ZKNodeAcl {

    private ZooKeeper zooKeeper = null;

    private static final Logger log = LoggerFactory.getLogger(ZKGetChildrenList.class);
    private static final String ZK_SERVER_URL = "127.0.0.1:2181";
    private static final int TIMEOUT = 5000;

    public ZKNodeAcl(String connectString) {
        try {
            this.zooKeeper = new ZooKeeper(connectString, TIMEOUT, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createZKNode(String path, byte[] data, List<ACL> acls) {
        try {
            String result = this.zooKeeper.create(path, data, acls, CreateMode.PERSISTENT);
            log.warn("创建节点：\t{}\t成功", result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws NoSuchAlgorithmException, KeeperException, InterruptedException {
        ZKNodeAcl zk = new ZKNodeAcl(ZK_SERVER_URL);

//        任何人都可以访问
        zk.createZKNode("/acl-imooc", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);

//        自定义用户认证访问
        List<ACL> acls = new ArrayList<>();
        Id imooc1 = new Id("digest", DigestAuthenticationProvider.generateDigest("imooc1:123456"));
        Id imooc2 = new Id("digest", DigestAuthenticationProvider.generateDigest("imooc2:123456"));
        acls.add(new ACL(ZooDefs.Perms.ALL, imooc1));
        acls.add(new ACL(ZooDefs.Perms.READ, imooc2));
        acls.add(new ACL(ZooDefs.Perms.DELETE | ZooDefs.Perms.CREATE, imooc2));
        zk.createZKNode("/acl-imooc/digest", "123".getBytes(), acls);

//        注册过的用户必须通过addAuthInfo才能操作节点
        zk.zooKeeper.addAuthInfo("digest", "imooc1:123456".getBytes());
        zk.createZKNode("/acl-imooc/digest/childtest", "child".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL);
        Stat stat = new Stat();
        byte[] data = zk.zooKeeper.getData("/acl-imooc/digest", false, stat);
        log.warn(new String(data));
        zk.zooKeeper.setData("/acl-imooc/digest", "now".getBytes(), 0);

//        ip方式的acl
        zk.zooKeeper.setData("/acl-imooc/iptest", "now".getBytes(), 0);
        data = zk.zooKeeper.getData("/acl-imooc/iptest", false, stat);
        log.warn(new String(data));
        log.warn(String.valueOf(stat.getVersion()));
    }
}
