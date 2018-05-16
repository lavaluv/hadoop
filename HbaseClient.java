import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseClient {
	public static void main(String[] args)throws IOException{
		Configuration configuration = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(configuration);
		Admin admin = connection.getAdmin();
		try {
			TableName tableName = TableName.valueOf("test");
			TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
					.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data")).build())
					.build();
			admin.createTable(htd);
			List<TableDescriptor> tables = admin.listTableDescriptors();
			if (tables.size() != 1 && Bytes.equals(tableName.getName(), 
					tables.get(0).getTableName().getName())) {
				throw new IOException("Fail create table");
			}
			//run some operations --put,get,scan
			Table table = connection.getTable(tableName);
			try {
				for(int i = 1; i <= 3; i++) {
					byte[] row = Bytes.toBytes("row" + i);
					Put put = new Put(row);
					byte[] column = Bytes.toBytes("data");
					byte[] qualifier = Bytes.toBytes(String.valueOf(i));
					byte[] value = Bytes.toBytes("value" + i);
					put.addColumn(column,qualifier,value);
					table.put(put);
				}
				Get get = new Get(Bytes.toBytes("row1"));
				Result re = table.get(get);
				System.out.println("get: " + re);
				Scan scan = new Scan();
				ResultScanner scanner = table.getScanner(scan);
				try {
					for(Result scannerResult:scanner) {
						System.out.println("Scan: " + scannerResult);
					}
				} finally {
					scanner.close();
				}
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}finally {
				table.close();
			}
		}finally {
			admin.close();
		}
	}
}
