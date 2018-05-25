import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ZKConfigWatcher implements Watcher{
	private ZKKeyValueStore keyValueStore;
	public ZKConfigWatcher(String hosts)throws InterruptedException,IOException{
		keyValueStore = new ZKKeyValueStore();
		keyValueStore.connect(hosts);
	}
	public void displayConfig()throws InterruptedException,KeeperException{
		String value = keyValueStore.read(ZKConfigUpdata.PATH, this);
		System.out.printf("Read %s as %s \n", ZKConfigUpdata.PATH,value);
	}
	@Override
	public void process(WatchedEvent event) {
		if(event.getType() == EventType.NodeDataChanged) {
			try {
				displayConfig();
			}catch(InterruptedException e){
				System.err.println("Interupted");
			}catch (KeeperException e) {
				System.err.printf("KeeperException: %s\n", e);
			}
		}
	}
	public static void main(String[] args)throws Exception{
		ZKConfigWatcher configWatcher = new ZKConfigWatcher("localhost");
		configWatcher.displayConfig();
		//Stay utill killed
		Thread.sleep(Long.MAX_VALUE);
	}
}
