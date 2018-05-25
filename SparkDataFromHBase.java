import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SparkDataFromHBase {
	public static void main(String[] args)throws Exception{
		SparkConf sConf = new SparkConf()
				.setAppName("SparkDataFormHBase")
				.setMaster("yarn");
		JavaSparkContext sContext = new JavaSparkContext(sConf);
		String tableName = "pkg";
		sContext.hadoopConfiguration().set("hbase.zookeeper.quorum","hadoop");
		sContext.hadoopConfiguration().set("hbase.zookeeper.property.clientPort","2181");
		sContext.hadoopConfiguration().set(TableInputFormat.INPUT_TABLE, tableName);
		
		Connection connection = ConnectionFactory.createConnection(sContext.hadoopConfiguration());
		Admin admin = connection.getAdmin();
		if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
			System.err.println(tableName+" is not exist.");
			System.exit(1);
		}
		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = 
				sContext.newAPIHadoopRDD(sContext.hadoopConfiguration(), 
						TableInputFormat.class,
						ImmutableBytesWritable.class, 
						Result.class);
//		Long count = hbaseRDD.count();
//		System.out.println(count);
		hbaseRDD.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable,Result>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<ImmutableBytesWritable, Result> result) throws Exception {
				// TODO Auto-generated method stub
				String key = Bytes.toString(result._2.getRow());
				String name = Bytes.toString(result._2.getValue("id".getBytes(), "name".getBytes()));
				//String age = Bytes.toString(result._2.getValue("id".getBytes(), "age".getBytes()));
				System.out.println("Row key:" + key+" Name:"+name);
			}
		});
		sContext.stop();
		sContext.close();
		admin.close();
	}
}
