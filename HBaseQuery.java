import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseQuery extends Configured implements Tool{
	public void getColumnFamilyName(String tableName) throws IOException {
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf(tableName));
		TableDescriptor tableDescriptor = table.getDescriptor();
		Set<byte[]> columnName = tableDescriptor.getColumnFamilyNames();
		Iterator<byte[]> iterator = columnName.iterator();
		System.out.println("The table "+tableName+"'s column are:");
		while (iterator.hasNext()) {
			String bs = new String(iterator.next());
			System.out.println(bs);
		}
	}
	public void getValue(String tableName,String columnName,int num) throws IOException {
		NavigableMap<String, String> resultMap = new TreeMap<String,String>();
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(columnName), Bytes.toBytes("1"));
		ResultScanner scanner = table.getScanner(scan);
		try {
			Result result;
			int count = 0;
			System.out.println(scanner.next() == null);
			while((result = scanner.next()) != null && count++ < num) {
				byte[] row = result.getRow();
				byte[] value = result.getValue(Bytes.toBytes(columnName), Bytes.toBytes("1"));
				System.out.println(new String(row)+ ":" + new String(value));
				resultMap.put(row.toString(), value.toString());
			}
		}finally {
			scanner.close();
		}
	}
	public static String[] getColumnName(String tableName,String columnFamilyName){
		return null;
	}
	public int run(String[] arg)throws IOException{
//		if (args.length < 1) {
//			System.err.println("Usage:HBaseQuery <method> <options>...");
//			return -1;
//		}
		String[] args = {"","","",""};
		args[0] = "getColumnFamilyName";
		args[1] = "pkg";
		args[2] = "url";
		switch (args[0]) {
		case "getColumnFamilyName":
			getColumnFamilyName(args[1]);
			break;
		case "getValue":
			getValue(args[1], args[2], 10);
			break;
		default:
			System.err.println("No such option.Please enter getColumnName|getValue");
			return -1;
		}
		return 0;
	}
	public static void main(String[] args)throws Exception{
		int exit = ToolRunner.run(HBaseConfiguration.create(), new HBaseQuery(), args);
		System.exit(exit);
	}
}
