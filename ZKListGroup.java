import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZKListGroup implements Watcher{
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
	public void list(String groupName)throws KeeperException,InterruptedException{
		String path = "/" + groupName;
		try {
			List<String> child = zk.getChildren(path, false);
			if(child.isEmpty()) {
				System.out.printf("No members in group %s\n", groupName);
				System.exit(1);
			}
			for(String c:child) {
				System.out.println(c);
			}
		}catch(KeeperException.NoNodeException e) {
			System.out.printf("Group %s does not exist\n", groupName);
			System.exit(1);
		}
	}
	public static void main(String[] args)throws Exception{
		ZKListGroup listGroup = new ZKListGroup();
		listGroup.connect(/*args[0]*/"localhost:2182");
		listGroup.list(/*args[1]*/"zoo");
		listGroup.close();
	}
}
