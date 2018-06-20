import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HRegionLocator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class SparkBulkLoad {
	public static final String HBASE_TABLE_NAME="pkg";
	private static final String HBASE_COLUMNFAMILY_NAME="value";
	public static void main(String[] args)throws Exception{
		Class<?> classes[] = {
				ImmutableBytesWritable.class,
				KeyValue.class,
				Put.class,
				ImmutableBytesWritable.Comparator.class
		};
		SparkConf sConf = new SparkConf()
				.setAppName("SparkDataIntoHBase")
				.setMaster("local")
				.registerKryoClasses(classes);
		JavaSparkContext sContext = new JavaSparkContext(sConf);
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","hadoop");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE_NAME);
		Connection connection = ConnectionFactory.createConnection(conf);
		
		Job job = Job.getInstance(conf);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		TableName tableName = TableName.valueOf(HBASE_TABLE_NAME.getBytes());
		Table table = connection.getTable(tableName);
		HRegionLocator regionLocator = new HRegionLocator(tableName, (ClusterConnection)connection);
		HFileOutputFormat2.configureIncrementalLoad(job, table,regionLocator);
		job.setOutputFormatClass(HFileOutputFormat2.class);
		job.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "hdfs://localhost:8020/hbase/bulkload");
		
		JavaRDD<String> inDataRDD = sContext.textFile(args[0]);
		
		JavaRDD<Map<?, ?>> rddArrays = inDataRDD
				.map(s -> RecordParser.parse(s));
		JavaPairRDD<ImmutableBytesWritable, KeyValue> pairRDD = rddArrays
				.mapToPair(new PairFunction<Map<?,?>, ImmutableBytesWritable, KeyValue>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<ImmutableBytesWritable, KeyValue> call(Map<?, ?> in) throws JsonParseException, JsonMappingException, IOException{
						if(in != null) {
							ObjectMapper mapper = new ObjectMapper();
							byte[] rowKey = RowKeyConverter.PTDRowKey(
									in.get("id").toString(), mapper.writeValueAsString(in.get("ts")));
							KeyValue keyValue = null;
							for(Map.Entry<?, ?> entry : in.entrySet()) {
								if (entry.getValue() != null && entry.getValue().toString() != "[]") {
									byte[] column = Bytes.toBytes(HBASE_COLUMNFAMILY_NAME);
									byte[] qualifier = Bytes.toBytes(entry.getKey().toString());
									byte[] value;
									if(entry.getValue().toString().indexOf("{") == -1) {
										value = Bytes.toBytes(entry.getValue().toString());
									}else {
										value = Bytes.toBytes(mapper.writeValueAsString(entry.getValue()));
									}
									keyValue = new KeyValue(rowKey, column, qualifier, value);
								}
							} 
							return new Tuple2<ImmutableBytesWritable, KeyValue>(
									new ImmutableBytesWritable(),keyValue);
						}else {
							return null;
						}
						
					}
				}).sortByKey();
		pairRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());
		sContext.stop();
		sContext.close();

	}
}
