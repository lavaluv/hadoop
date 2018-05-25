import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

public class ZKConfigUpdata {
	public static final String PATH = "/config";
	
	private ZKKeyValueStore keyValueStore;
	private Random random = new Random();
	
	public ZKConfigUpdata(String hosts)throws IOException,InterruptedException{
		keyValueStore = new ZKKeyValueStore();
		keyValueStore.connect(hosts);
	}
	public void run()throws InterruptedException,KeeperException{
		while(true) {
			String value = random.nextInt(100) + "";
			keyValueStore.write(PATH, value);
			System.out.printf("Set %s to %s\n",	PATH,value);
			TimeUnit.SECONDS.sleep(random.nextInt(10));
		}
	}
	public static void main(String[] args)throws Exception{
		ZKConfigUpdata configUpdata = new ZKConfigUpdata("localhost");
		configUpdata.run();
	}
}
