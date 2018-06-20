import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import com.sematext.hbase.wd.DistributedScanner;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;
import com.twitter.chill.Base64;


public class SparkDataFromHBase {
	private static final String HBASE_TABLE_NAME="pkg";
	private static final String HBASE_COLUMNFAMILY_NAME="value";
	private static final String HBASE_QUALIFIER="ts";
	@SuppressWarnings("unused")
	public static void main(String[] args)throws Exception{
		SparkConf sConf = new SparkConf()
				.setAppName("SparkDataFormHBase")
				.setMaster("local");
		JavaSparkContext sContext = new JavaSparkContext(sConf);
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum","hadoop");
		configuration.set("hbase.zookeeper.property.clientPort","2181");
		configuration.set(TableInputFormat.INPUT_TABLE, HBASE_TABLE_NAME);
		
		Connection connection = ConnectionFactory.createConnection(configuration);
		Admin admin = connection.getAdmin();
		Table table = connection.getTable(TableName.valueOf(HBASE_TABLE_NAME));
		if (!admin.isTableAvailable(TableName.valueOf(HBASE_TABLE_NAME))) {
			System.err.println(HBASE_TABLE_NAME+" is not exist.");
			System.exit(1);
		}
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(HBASE_COLUMNFAMILY_NAME),Bytes.toBytes(HBASE_QUALIFIER));
		//query like: equal to a family:qualifier:value
		ValueFilter valueFilter = new ValueFilter(
				CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("cap")));
		//when the query is complicated
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		//will return a row that matched
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes("value"), 
				Bytes.toBytes("type"), 
				CompareOperator.EQUAL, 
				Bytes.toBytes("cap"));
		//only return a cell that matched
		ColumnValueFilter filter2 = new ColumnValueFilter(
				Bytes.toBytes("value"), 
				Bytes.toBytes("threat_level"), 
				CompareOperator.EQUAL, 
				Bytes.toBytes("3"));
		filterList.addFilter(filter1);
		//filterList.addFilter(filter2);
		//scan.setFilter(valueFilter);
		//scan.setFilter(filterList);
		
		AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(
				new RowKeyDistributorByHashPrefix.OneByteSimpleHash(10));
		ResultScanner scanner = DistributedScanner.create(table, scan, keyDistributor);
		
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		String scanToString = Base64.encodeBytes(proto.toByteArray());
		configuration.set(TableInputFormat.SCAN, scanToString);
		
		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = 
				sContext.newAPIHadoopRDD(configuration, 
						TableInputFormat.class,
						ImmutableBytesWritable.class, 
						Result.class);
		JavaRDD<String> resultRDD = hbaseRDD.map(tuple -> {
			Result result = tuple._2();
			byte[] row = result.getRow();
			byte[] value = result.getValue(Bytes.toBytes(HBASE_COLUMNFAMILY_NAME), Bytes.toBytes(HBASE_QUALIFIER));
			System.out.println(new String(row)+ ":" + new String(value));
			return new String(row)+ ":" + new String(value);
		});
		//resultRDD.take(HBASE_NUM);
		System.out.println(resultRDD.count());

		sContext.stop();
		sContext.close();
		admin.close();
	}
}
