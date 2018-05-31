import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SSTest {
	public static void main(String[] args)throws Exception{
		SparkConf sConf = new SparkConf()
				.setAppName("SSTest")
				.setMaster("local");
		JavaStreamingContext jssc = new JavaStreamingContext(sConf,Durations.seconds(1));
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("192.168.19.143", 9999);
		JavaDStream<String> words = lines.flatMap(
				x -> Arrays.asList(x.split(" ")).iterator());
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
				(i1,i2) -> i1 + i2);
		
		wordCounts.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
		jssc.close();
	}
}
