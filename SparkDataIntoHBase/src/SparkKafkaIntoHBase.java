import java.io.IOException;
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
//import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class SparkKafkaIntoHBase {
	private static final String HBASE_PKA_NAME="test";
	private static final String[] KAFKA_TOPIC_NAME= {"test"};
	private static final String HBASE_COLUMNFAMILY_NAME="v";
	public static void main(String[] args)throws Exception{
		//for possible kafkaParam,see http://kafka.apache.org/documentation.html#newconsumerconfigs
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "192.168.18.143:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		//group_id must be separate
		kafkaParams.put("group.id", "spark");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);
		
		//get data from topics
		System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		Collection<String> topics = Arrays.asList(KAFKA_TOPIC_NAME);
		SparkConf sConf = new SparkConf()
				.setAppName("SparkKafkaIntoHBase")
				.setMaster("yarn");
		//The batch duration must be same as kafka heartbeat.interval.ms and session.timeout.ms
		JavaStreamingContext jssc = new JavaStreamingContext(sConf,Durations.milliseconds(1000));
		//set hbase configuration
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum","slave2");
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
		//one kafka partition:one sparkRDD:one task:your executor core num
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
			JavaPairRDD<ImmutableBytesWritable, Put> outData = out
			//reparition over all executors
			//.repartition(4)
			.mapToPair(new PairFunction<ArrayList<Map<?,?>>, ImmutableBytesWritable, Put>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Tuple2<ImmutableBytesWritable, Put> call (ArrayList<Map<?,?>> indata) throws IOException{
					Put put = null;
					for(Map<?, ?> in:indata) {
						ObjectMapper mapper = new ObjectMapper();
						byte[] rowKey = RowKeyConverter.PTDRowKey(in.get("id").toString(), mapper.writeValueAsString(in.get("ts")));
						put = new Put(rowKey);
						for(Map.Entry<?, ?> entry : in.entrySet()) {
							if (entry.getValue() != null && entry.getValue().toString() != "[]") {
								byte[] column = Bytes.toBytes(HBASE_COLUMNFAMILY_NAME);
								byte[] qualifier = Bytes.toBytes(entry.getKey().toString());
								byte[] value;
								if(entry.getValue().toString().indexOf("{") == -1 && entry.getValue().toString().indexOf("[") == -1) {
									value = Bytes.toBytes(entry.getValue().toString());
								}else {
									value = Bytes.toBytes(mapper.writeValueAsString(entry.getValue()));
								}
								put.addColumn(column, qualifier, value);
							}
						}
					}
					return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
				}
			});
			//outData.take(4000);
			outData.saveAsNewAPIHadoopDataset(job.getConfiguration());
			//updata offset
			//TODO
		});
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
		jssc.close();
	}
}
