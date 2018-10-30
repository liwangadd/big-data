package cn.windylee.curator.leader;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

public class LeaderSelectorExample {

    private static final int CLIENT_QTY = 10;
    private static final String PATH = "/windylee/leader";

    private static final Logger log = LoggerFactory.getLogger(LeaderSelectorExample.class);

    public static void main(String[] args) throws Exception {
        log.info("Create {} clients, have each negotiate for leadership and then wait a random number of seconds " +
                "before letting another leader election occur.", CLIENT_QTY);
        log.info("Notice that leader election is fair: all clients will become leader and will do so the same number of times.");

        List<CuratorFramework> clients = Lists.newArrayList();
        List<ClientExample> examples = Lists.newArrayList();

        TestingServer server = new TestingServer();

        try {
            for (int i = 0; i < CLIENT_QTY; i++) {
                CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
                clients.add(client);

                ClientExample example = new ClientExample(client, PATH, "Client #" + i);
                examples.add(example);

                client.start();
                example.start();
            }

            log.info("Press enter/return to quit");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } finally {
            log.info("Shutting down...");
            for (ClientExample clientExample : examples) {
                CloseableUtils.closeQuietly(clientExample);
            }
            for (CuratorFramework client : clients) {
                client.close();
            }
            CloseableUtils.closeQuietly(server);
        }
    }

}
