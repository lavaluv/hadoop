import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class SparkKafkaTest {
	private static final String HBASE_PKA_NAME="pkg";
	private static final String[] KAFKA_TOPIC_NAME= {"ptd_data"};
	public static void main(String[] args)throws Exception{
		//for possible kafkaParam,see http://kafka.apache.org/documentation.html#newconsumerconfigs
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "192.168.106.129:9092,anotherhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		//group_id must be separate
		kafkaParams.put("group.id", "spark");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", false);
		
		//get data from topics
		Collection<String> topics = Arrays.asList(KAFKA_TOPIC_NAME);
		SparkConf sConf = new SparkConf()
				.setAppName("SparkKafkaTest")
				.setMaster("local[2]");
		//The batch duration must be same as kafka heartbeat.interval.ms and session.timeout.ms
		JavaStreamingContext jssc = new JavaStreamingContext(sConf,Durations.milliseconds(500));
		//set hbase configuration
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum","hadoop");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.set(TableOutputFormat.OUTPUT_TABLE, HBASE_PKA_NAME);
		
		//set hadoop job configuration
		Job job = Job.getInstance(conf);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		
//		Map<TopicPartition, Long> fromOffsets = new HashMap<>();
//		fromOffsets.put(new TopicPartition("ptd_data", 0), Long.valueOf(0));
		//create direct stream from kafka
		JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(jssc,
						//distribute partitions evenly across available executors
						LocationStrategies.PreferConsistent(),
						//properly configured consumers
						ConsumerStrategies.<String,String>Subscribe(topics, kafkaParams)
						//ConsumerStrategies.<String,String>Assign(fromOffsets.keySet(), kafkaParams,fromOffsets)
						);
		stream.foreachRDD(rdd ->{
			//get kafka offset
//			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//			rdd.foreachPartition(consumerRecords ->{
//				OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
//				System.out.println(o.topic()+ " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
//			});
			//do caculation
			JavaRDD<ArrayList<Map<?,?>>> out = rdd.map(record ->
					RecordParser.ptd_parse(record.value()));
			JavaPairRDD<String, String> outData = out
					.mapToPair(new PairFunction<ArrayList<Map<?,?>>, String, String>() {
						private static final long serialVersionUID = 1L;
						@Override
						public Tuple2<String, String> call (ArrayList<Map<?,?>> indata) throws JsonProcessingException{
							for(Map<?, ?> in:indata) {
								for(Map.Entry<?, ?> entry : in.entrySet()) {
									//byte[] column = Bytes.toBytes(HBASE_COLUMNFAMILY_NAME);
									byte[] qualifier = Bytes.toBytes(entry.getKey().toString());
									byte[] value = Bytes.toBytes("");
									if (entry.getValue() != null) {
										ObjectMapper mapper = new ObjectMapper();
										value = Bytes.toBytes(mapper.writeValueAsString(entry.getValue()));
									}
									System.out.printf("_________________key::%s--------value::%s\n", 
											new String(qualifier),new String(value));
								}
							}
							return new Tuple2<String, String>(" ", "");
						}
					});
			outData.take(100);
		});
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
		jssc.close();
	}
}
