import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import com.sematext.hbase.wd.DistributedScanner;
import com.sematext.hbase.wd.RowKeyDistributorByBucket;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;

public class HBaseQuery extends Configured implements Tool{
	/**
	 * return all familyName by tableName
	 * @param tableName HBase table name
	 * @throws IOException
	 */
	public static void getColumnFamilyName(String tableName) throws IOException {
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
	/**
	 * return a Map contain the result by query : table/family/qualifier/value
	 * @param tableName
	 * @param columnFamilyName
	 * @param qualifier
	 * @param startRow 
	 * @param endRow
	 * @param valueFilter
	 * @param num
	 * @throws IOException
	 */
	public static void getValueByBucket(String tableName,String columnFamilyName,String qualifier,String startRow,String endRow,ValueFilter valueFilter,int num,int BucktetSize) throws IOException {
		NavigableMap<String, String> resultMap = new TreeMap<String,String>();
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		scan.withStartRow(Bytes.toBytes(startRow));
		scan.withStopRow(Bytes.toBytes(endRow));
		scan.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier));
		if(valueFilter != null) {
			scan.setFilter(valueFilter);
		}
		RowKeyDistributorByBucket keyDistributor = new RowKeyDistributorByBucket(BucktetSize);
		ResultScanner scanner = DistributedScanner.create(table, scan, keyDistributor);
		try {
			Result result;
			int count = 0;
			while((result = scanner.next()) != null && count++ < num) {
				byte[] row = result.getRow();
				byte[] value = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier));
				//System.out.println(new String(keyDistributor.getOriginalKey(row))+ ":" + new String(value));
				//resultMap.put(new String(keyDistributor.getOriginalKey(row)), new String(value));
				resultMap.put((new String(keyDistributor.getOriginalKey(row))).replace("x", ""), new String(value));
			}
			showQueryResult(resultMap,qualifier);
			System.out.println("Return result count is:" + resultMap.size());
		}finally {
			scanner.close();
		}
	}
	public static void getValueByHash(String tableName,String columnFamilyName,String qualifier,String startRow,String endRow,ValueFilter valueFilter,int num,int BucktetSize) throws IOException {
		NavigableMap<String, String> resultMap = new TreeMap<String,String>();
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		scan.withStartRow(Bytes.toBytes(startRow));
		scan.withStopRow(Bytes.toBytes(endRow));
		scan.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier));
		if(valueFilter != null) {
			scan.setFilter(valueFilter);
		}
		AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(
				new RowKeyDistributorByHashPrefix.OneByteSimpleHash(BucktetSize));
		ResultScanner scanner = DistributedScanner.create(table, scan, keyDistributor);
		try {
			Result result;
			int count = 0;
			while((result = scanner.next()) != null && count++ < num) {
				byte[] row = result.getRow();
				byte[] value = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier));
				resultMap.put(new String(keyDistributor.getOriginalKey(row)), new String(value));
			}
			showQueryResult(resultMap,qualifier);
			System.out.println("Return result count is:" + resultMap.size());
		}finally {
			scanner.close();
		}
	}
	/**
	 * return a Map contain result by query use filterlist
	 * @param tableName
	 * @param columnFamilyName
	 * @param resultQualifier
	 * @param startRow
	 * @param endRow
	 * @param flist
	 * @param num
	 * @throws IOException
	 */
	public static void getValueByFilter(String tableName,String columnFamilyName,String resultQualifier,String startRow,String endRow,FilterList flist,int num,int BucketSize) throws IOException {
		NavigableMap<String, String> resultMap = new TreeMap<String,String>();
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		scan.withStartRow(Bytes.toBytes(startRow));
		scan.withStopRow(Bytes.toBytes(endRow));
		if(flist != null) {
			scan.setFilter(flist);
		}
		AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByBucket(BucketSize);
		ResultScanner scanner = DistributedScanner.create(table, scan, keyDistributor);
		try {
			Result result;
			int count = 0;
			while((result = scanner.next()) != null && count++ < num) {
				byte[] row = result.getRow();
				byte[] value = result.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(resultQualifier));
				//System.out.println(new String(row)+ ":" + new String(value));
				resultMap.put((new String(keyDistributor.getOriginalKey(row))).replace("x",""), new String(value));
			}
			showQueryResult(resultMap,resultQualifier);
			System.out.println("Return result count is:" + resultMap.size());
		}finally {
			scanner.close();
		}
	}
	public static void getRowByFilter(String tableName,String columnFamilyName,String resultQualifier,String startRow,String endRow,FilterList flist,int num,int BucketSize) throws IOException{
		List<NavigableMap<byte[], byte[]>> resultMap = new ArrayList<>();
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		scan.withStartRow(Bytes.toBytes(startRow));
		scan.withStopRow(Bytes.toBytes(endRow));
		if(flist != null) {
			scan.setFilter(flist);
		}
		AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(
				new RowKeyDistributorByHashPrefix.OneByteSimpleHash(BucketSize));
		ResultScanner scanner = DistributedScanner.create(table, scan, keyDistributor);
		try {
			Result result;
			int count = 0;
			while((result = scanner.next()) != null && count++ < num) {
				NavigableMap<byte[], byte[]> re = result.getFamilyMap(Bytes.toBytes("value"));
				for(Map.Entry<byte[], byte[]> entry:re.entrySet()) {
					System.out.println(new String(entry.getKey())+":"+new String(entry.getValue()));
				}
				resultMap.add(re);
			}
			System.out.println("Return result count is:" + resultMap.size());
		}finally {
			scanner.close();
		}
	}
	public static void getValueByGet(String tableName,String columnFamilyName,String resultQualifier,String startRow,String endRow,FilterList flist,int num) throws IOException {
		NavigableMap<String, String> resultMap = new TreeMap<String,String>();
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		Table table = connection.getTable(TableName.valueOf(tableName));
		ArrayList<Get> re = new ArrayList<>();
		int count = 0;
		System.out.println(Long.parseLong(startRow)+":"+Long.parseLong(endRow));
		for(long i = Long.parseLong(startRow);i < Long.parseLong(endRow);i++) {
			count++;
			Get get = new Get(Long.toString(i).getBytes());
			if (flist != null) {
				get.setFilter(flist);
			}
			re.add(get);
			if(count <= num) {
				
			}
		}
		Result[] result = table.get(re);
		System.out.println(result.length+":"+re.size());
		for (int i = 0; i < result.length; i++) {
			byte[] row = result[i].getRow();
			byte[] value = result[i].getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes(resultQualifier));
			System.out.println(new String(row)+ ":" + new String(value));
			resultMap.put(resultQualifier, new String(value));
		}
		System.out.println("Return result count is:" + resultMap.size());
	}
	public static void showQueryResult(Map<String, String> re,String qualifier) {
		for(Map.Entry<String, String> entry:re.entrySet()) {
			System.out.println(entry.getKey()+":"+qualifier+":"+entry.getValue());
		}
	}
	public static FilterList filterComponent (String operator,List<String[]> filters) {
		FilterList.Operator o = null;
		switch (operator) {
		case "MUST_PASS_ONE":
			o = Operator.MUST_PASS_ONE;
			break;
		case "MUST_PASS_ALL":
			o = Operator.MUST_PASS_ALL;
			break;
		default:
			System.err.println("Err input in filterComponent.");
			return null;
		}
		FilterList filterList = new FilterList(o);
		Iterator<String[]> iterator = filters.iterator();
		while(iterator.hasNext()) {
			String[] string = iterator.next();
			CompareOperator cOperator = null;
			switch (string[1]) {
			case "=":
				cOperator = CompareOperator.EQUAL;
				break;
			case "!=":
				cOperator = CompareOperator.NOT_EQUAL;
				break;
			case ">":
				cOperator = CompareOperator.GREATER;
				break;
			case "<":
				cOperator = CompareOperator.LESS;
				break;
			case ">=":
				cOperator = CompareOperator.GREATER_OR_EQUAL;
				break;
			case "<=":
				cOperator = CompareOperator.LESS_OR_EQUAL;
				break;
			case "no":
				cOperator = CompareOperator.NO_OP;
				break;
			default:
				System.err.println("Err input in filterComponent.");
				return null;
			}
			if (string[0] == "1") {
				//will return a row that matched
				SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
						Bytes.toBytes(string[2]), 
						Bytes.toBytes(string[3]), 
						cOperator, 
						Bytes.toBytes(string[4]));
				filterList.addFilter(singleColumnValueFilter);
			}else if(string[0] == "2") {
				//only return a cell that matched
				ColumnValueFilter columnValueFilter = new ColumnValueFilter(
						Bytes.toBytes(string[2]), 
						Bytes.toBytes(string[3]), 
						CompareOperator.EQUAL, 
						Bytes.toBytes(string[4]));
				filterList.addFilter(columnValueFilter);
			}else if(string[0] == "3") {
				//return a row that family:qualifier:value match your regex
				RegexStringComparator regexFilter = new RegexStringComparator(string[4]);
				SingleColumnValueFilter complexRegexFilter = new SingleColumnValueFilter(
						Bytes.toBytes(string[2]), 
						Bytes.toBytes(string[3]), 
						CompareOperator.EQUAL,
						regexFilter);
				filterList.addFilter(complexRegexFilter);
			}else {
				System.err.println("Err input in filterComponent.");
			}
		}
		return filterList;
	}
	public static ValueFilter simpleFilterComponent (String[] filter) {
		CompareOperator cOperator = null;
		switch (filter[1]) {
		case "=":
			cOperator = CompareOperator.EQUAL;
			break;
		case "!=":
			cOperator = CompareOperator.NOT_EQUAL;
			break;
		case ">":
			cOperator = CompareOperator.GREATER;
			break;
		case "<":
			cOperator = CompareOperator.LESS;
			break;
		case ">=":
			cOperator = CompareOperator.GREATER_OR_EQUAL;
			break;
		case "<=":
			cOperator = CompareOperator.LESS_OR_EQUAL;
			break;
		case "no":
			cOperator = CompareOperator.NO_OP;
			break;
		default:
			System.err.println("Err input in simpleFilterComponent.");
			return null;
		}
		ValueFilter valueFilter = null;
		if (filter[0] == "1") {
			valueFilter = new ValueFilter(
					cOperator,new BinaryComparator(Bytes.toBytes(filter[2])));
		}else if(filter[0] =="2") {
			valueFilter = new ValueFilter(
					cOperator, new RegexStringComparator(filter[2]));
		}else {
			System.err.println("Err input in simpleFilterComponent.");
		}
		return valueFilter;
	}
	@SuppressWarnings("unused")
	public int run(String[] arg)throws IOException{
//		if (args.length < 1) {
//			System.err.println("Usage:HBaseQuery <method> <options>...");
//			return -1;
//		}
		String[] args = {"","","","","","",""};
		args[0] = "getValueByBucket";
		args[1] = "test";
		args[2] = "v";
		args[3] = "dst";
		args[4] = "201806170900";
		args[5] = "201806181003";
		//query like: equal to a family:qualifier:value
		String[] simpleValue = {"2","=","\"中国\""};
		ValueFilter valueFilter= simpleFilterComponent(simpleValue);
		//when the query is complicated
		List<String[]> filterValue = new ArrayList<>();
		String[] value = {"1","=","v","proto","[\"HTTP\",\"TCP\"]"};
		String[] value2 = {"3","=","v","dst","\"中国\""};
		filterValue.add(value);
		filterValue.add(value2);
		FilterList filterList = filterComponent("MUST_PASS_ALL", filterValue);
		switch (args[0]) {
		case "getColumnFamilyName":
			getColumnFamilyName(args[1]);
			break;
		case "getValueByBucket":
			getValueByBucket(args[1], args[2], args[3],args[4],args[5],valueFilter,Integer.MAX_VALUE,4);
			break;
		case "getValueByFilter":
			getValueByFilter(args[1], args[2], args[3],args[4],args[5],filterList,Integer.MAX_VALUE,4);
			break;
		case "getValueByGet":
			getValueByGet(args[1], args[2], args[3], args[4], args[5], null, Integer.MAX_VALUE);
			break;
		case "getValueByHash":
			getValueByHash(args[1], args[2], args[3],args[4],args[5],null,Integer.MAX_VALUE,4);
			break;
		case "getRowByFliter":
			getRowByFilter(args[1], args[2], args[3],args[4],args[5],null,Integer.MAX_VALUE,4);
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
