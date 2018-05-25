import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZKJoinGroup implements Watcher{
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
	public void join(String groupName,String memberName)throws KeeperException,InterruptedException{
		String path = '/' + groupName + "/" + memberName;
		String createPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
		System.out.println(createPath);
	}
	public static void main (String[] args)throws Exception{
		ZKJoinGroup joinGroup = new ZKJoinGroup();
		joinGroup.connect("localhost:2183");
		joinGroup.join("zoo", "z3");
		//stay alive utill process is killed or interupted
		Thread.sleep(Long.MAX_VALUE);
	}
}
