import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
//import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

//import scala.Tuple2;

public class SparkKafkaIntoHBase {
	private static final String HBASE_PKA_NAME="pkg";
	private static final String[] KAFKA_TOPIC_NAME= {"topicB"};
	public static void main(String[] args)throws Exception{
		//for possible kafkaParam,see http://kafka.apache.org/documentation.html#newconsumerconfigs
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		//group_id must be separate
		kafkaParams.put("group.id", "my_group");
		kafkaParams.put("auto.offset.reset", "latest");
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
		
		//create direct stream from kafka
		JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(jssc,
						//distribute partitions evenly across available executors
						LocationStrategies.PreferConsistent(),
						//properly configured consumers
						ConsumerStrategies.<String,String>Subscribe(topics, kafkaParams)
						);
		stream.foreachRDD(rdd ->{
			//get kafka offset
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			rdd.foreachPartition(consumerRecords ->{
				OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
				System.out.println(o.topic()+ " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
			});
			//do caculation
//			JavaRDD<Map<?,?>> inData = rdd.map(record ->
//					RecordParser.parse(record.value()));
//			JavaPairRDD<ImmutableBytesWritable, Put> pairRDD = inData
//					.mapToPair(new PairFunction<Map<?,?>, ImmutableBytesWritable, Put>() {
//						private static final long serialVersionUID = 1L;
//						@Override
//						public Tuple2<ImmutableBytesWritable, Put> call(Map<?, ?> in){
//							byte[] rowKey = RowKeyConverter.makeInfoRowKey(
//									in.get("id").toString(), in.get("ts").toString());
//							Put put = new Put(rowKey);
//							for(Map.Entry<?, ?> entry : in.entrySet()) {
//								byte[] column = Bytes.toBytes(entry.getKey().toString());
//								byte[] qualifier = Bytes.toBytes(String.valueOf(1));
//								byte[] value = Bytes.toBytes(entry.getValue().toString());
//								put.addColumn(column,qualifier,value);
//							}
//							return new Tuple2<ImmutableBytesWritable, Put>(
//									new ImmutableBytesWritable(),put);
//						}
//					});
//			pairRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());
			//updata offset
			//TODO
		});
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
		jssc.close();
	}
}
