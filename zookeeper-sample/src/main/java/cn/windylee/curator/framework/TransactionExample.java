package cn.windylee.curator.framework;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;

import java.util.Collection;
import java.util.List;

public class TransactionExample {

    public static Collection<CuratorTransactionResult> transaction(CuratorFramework client) throws Exception {
        CuratorOp createOp = client.transactionOp().create().forPath("/a/path", "some data".getBytes());
        CuratorOp setDataOp = client.transactionOp().setData().forPath("/another/path", "other data".getBytes());
        CuratorOp deleteOp = client.transactionOp().delete().forPath("/yet/another/data");

        List<CuratorTransactionResult> results = client.transaction().forOperations(createOp, setDataOp, deleteOp);

        for (CuratorTransactionResult result : results) {
            System.out.println(result.getForPath() + " - " + result.getType());
        }
        return results;
    }

}
