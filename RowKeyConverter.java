import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyConverter {
	/**
	 * return a rowKey that format is:<id> <ts.end>
	 */
	public static byte[] makeInfoRowKey(String id,String tsEnd) {
		byte[] row = new byte[id.length()];
		Bytes.putBytes(row, 0, Bytes.toBytes(id), 0, id.length());
		//long reverseTime = 2;//Long.MAX_VALUE - Long.parseLong(tsEnd);
		//Bytes.putLong(row, id.length(), reverseTime);
		return row;
	}
}
