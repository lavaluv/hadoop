import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZKDeleteGroup implements Watcher{
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
	public void delete(String groupName)throws KeeperException,InterruptedException{
		String path = "/" + groupName;
		try {
			List<String> child = zk.getChildren(path, false);
			for(String cString : child) {
				zk.delete(path + "/" +cString, -1);
			}
			zk.delete(path, -1);
		}catch (KeeperException.NoNodeException e) {
			System.out.printf("Group %s does not exist\n", groupName);
			System.exit(1);
		}
	}
	public static void main(String[] args)throws Exception{
		ZKDeleteGroup deleteGroup = new ZKDeleteGroup();
		deleteGroup.connect("localhost:2181");
		deleteGroup.delete("zoo");
		deleteGroup.close();
	}
}
