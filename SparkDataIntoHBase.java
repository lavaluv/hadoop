import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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

public class SparkDataIntoHBase {
	public static final String HBASE_TABLE_NAME="pkg";
	private static final String HBASE_COLUMNFAMILY_NAME="value";
	public static void main(String[] args)throws Exception{
		SparkConf sConf = new SparkConf()
				.setAppName("SparkDataIntoHBase")
				.setMaster("local");
		JavaSparkContext sContext = new JavaSparkContext(sConf);
		sContext.hadoopConfiguration().set("hbase.zookeeper.quorum","hadoop");
		sContext.hadoopConfiguration().set("hbase.zookeeper.property.clientPort","2181");
		sContext.hadoopConfiguration().set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE_NAME);
		
		Job job = Job.getInstance(sContext.hadoopConfiguration());
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		
		JavaRDD<String> inDataRDD = sContext.textFile(args[0]);
		
		JavaRDD<Map<?, ?>> rddArrays = inDataRDD
				.map(s -> RecordParser.parse(s));
		JavaPairRDD<ImmutableBytesWritable, Put> pairRDD = rddArrays
				.mapToPair(new PairFunction<Map<?,?>, ImmutableBytesWritable, Put>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<ImmutableBytesWritable, Put> call(Map<?, ?> in) throws JsonParseException, JsonMappingException, IOException{
						if(in != null) {
							ObjectMapper mapper = new ObjectMapper();
							byte[] rowKey = RowKeyConverter.PTDRowKey(
									in.get("id").toString(), mapper.writeValueAsString(in.get("ts")));
							Put put = new Put(rowKey);
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
							return new Tuple2<ImmutableBytesWritable, Put>(
									new ImmutableBytesWritable(),put);
						}else {
							return null;
						}
						
					}
				});
		pairRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());
		sContext.stop();
		sContext.close();

	}
}
