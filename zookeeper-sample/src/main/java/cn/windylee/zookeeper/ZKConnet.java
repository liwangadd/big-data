package cn.windylee.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ZKConnet implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(ZKConnet.class);

    private static final String ZK_SERVER_PATH = "127.0.0.1:2181";
    private static final Integer timeout = 5000;

    public static void main(String[] args) throws IOException, InterruptedException {
        /**
         * 客户端和zk服务端连接是一个异步过程
         * 当连接成功后，客户端会收到一个watch通知
         */
        ZooKeeper zk = new ZooKeeper(ZK_SERVER_PATH, timeout, new ZKConnet());

        long sessionId = zk.getSessionId();
        byte[] sessionPasswd = zk.getSessionPasswd();

        log.warn("客户端开始连接zookeeper服务器...");
        log.warn("连接状态: {}", zk.getState());

        Thread.sleep(2000);
        log.warn("连接状态: {}", zk.getState());

        Thread.sleep(200);
        log.warn("开始会话重连...");

        /**
         * 参数：
         * connectString：zookeeper的服务器地址，{ip：port}，可以是一个也可以是多个（使用,分隔）
         * sessionTimeout：会话超时时间，超过这个时间收不到心跳连接，该session失效
         * watcher：通知时间，如果有对应的时间触发，则会收到一个通知；如果不需要接收通知，可以设置为null
         * canBeReadOnly：当某个服务器出现问题，不能提供写服务但可以提供读服务，是否继续读取数据（读取到的可能是旧数据），建议设置为false
         * sessionId：会话id
         * sessionPasswd：会话密码，当会话丢失后，可以根据sessionId和sessionPasswd恢复会话
         */
        ZooKeeper zkSession = new ZooKeeper(ZK_SERVER_PATH, timeout, new ZKConnet(), sessionId, sessionPasswd);
        log.warn("重新连接状态zkSession: {}", zkSession.getState());
        Thread.sleep(1000);
        log.warn("重新连接状态zkSession: {}", zkSession.getState());
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.warn("接收到watch通知：{}", watchedEvent);
    }
}
