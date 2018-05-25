import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

public class SparkKafkaOffsets {
	public static void main(String[]args)throws Exception{
		//for possible kafkaParam,see http://kafka.apache.org/documentation.html#newconsumerconfigs
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		//group_id must be separate
		kafkaParams.put("group.id", "my_group");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		SparkConf sConf = new SparkConf()
				.setAppName("SparkKafkaOffset")
				.setMaster("local");
		JavaSparkContext jContext = new JavaSparkContext(sConf);
		
		OffsetRange[] offsetRanges = {
				//topic, patition, inclusive starting offset, exclusive ending offset
				OffsetRange.create("test", 0, 0,100),
				OffsetRange.create("test", 1, 0,100)
		};
		@SuppressWarnings("unused")
		JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
				jContext,
				kafkaParams,
				offsetRanges,
				LocationStrategies.PreferConsistent());
	};
}
