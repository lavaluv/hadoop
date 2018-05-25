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

import scala.Tuple2;

public class SparkDataIntoHBase {
	public static void main(String[] args)throws Exception{
		SparkConf sConf = new SparkConf()
				.setAppName("SparkDataIntoHBase")
				.setMaster("yarn");
		JavaSparkContext sContext = new JavaSparkContext(sConf);
		String tableName = "pkg";
		sContext.hadoopConfiguration().set("hbase.zookeeper.quorum","hadoop");
		sContext.hadoopConfiguration().set("hbase.zookeeper.property.clientPort","2181");
		sContext.hadoopConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
		
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
					public Tuple2<ImmutableBytesWritable, Put> call(Map<?, ?> in){
						byte[] rowKey = RowKeyConverter.makeInfoRowKey(
								in.get("id").toString(), in.get("ts").toString());
						Put put = new Put(rowKey);
						for(Map.Entry<?, ?> entry : in.entrySet()) {
							byte[] column = Bytes.toBytes(entry.getKey().toString());
							byte[] qualifier = Bytes.toBytes(String.valueOf(1));
							byte[] value = Bytes.toBytes(entry.getValue().toString());
							put.addColumn(column,qualifier,value);
						}
						return new Tuple2<ImmutableBytesWritable, Put>(
								new ImmutableBytesWritable(),put);
					}
				});
		pairRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());
		sContext.stop();
		sContext.close();

	}
}
