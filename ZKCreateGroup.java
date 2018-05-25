import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZKCreateGroup implements Watcher{
	private static final int SESSION_TEIME_OUT = 5000;
	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	public void connect(String hosts) throws IOException,InterruptedException{
		zk = new ZooKeeper(hosts, SESSION_TEIME_OUT, this);
		//Prevent the usage of zk before created
		connectedSignal.await();
	}
	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			//when connectedSignal is 0 , await() return
			connectedSignal.countDown();
		}
	}
	public void close() throws InterruptedException{
		zk.close();
	}
	public static void main(String[] args)throws Exception {
		ZKCreateGroup createGroup = new ZKCreateGroup();
		createGroup.connect("localhost:2181");
		createGroup.create("zoo");
		createGroup.close();
		System.exit(0);
	}
	public void create(String groupName)throws KeeperException,InterruptedException{
		String path = "/" + groupName;
		String createdPath = zk.create(
				path, null/*data*/, Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		System.out.println("Created " + createdPath);
	}
}
