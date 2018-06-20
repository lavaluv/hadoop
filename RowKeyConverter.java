import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;
import com.sematext.hbase.wd.RowKeyDistributorByOneBytePrefix;

public class RowKeyConverter {
	/**
	 * 
	 * @param id
	 * @param timestamp
	 * @return a rowkey like:<timestamp><id>
	 * @throws IOException
	 */
	public static byte[] makeInfoRowKey(String id,String timestamp) throws IOException {
		byte[] row = new byte[id.length()+timestamp.length()];
		//long reverseTime = Long.MAX_VALUE - Long.parseLong(timestamp);
		//Bytes.putLong(row, 0, reverseTime);
		Bytes.putBytes(row, 0, Bytes.toBytes(timestamp),0,timestamp.length());
		Bytes.putBytes(row, timestamp.length(), Bytes.toBytes(id), 0, id.length());
		byte bucketCount = (byte) 10;
		RowKeyDistributorByOneBytePrefix kb = new RowKeyDistributorByOneBytePrefix(bucketCount);
		return kb.getDistributedKey(row);
	}
	public static byte[] makeHaseRowKey(String id,String timestamp) throws IOException{
		byte[] row = new byte[id.length()+timestamp.length()];
		Bytes.putBytes(row, 0, Bytes.toBytes(timestamp),0,timestamp.length());
		Bytes.putBytes(row, timestamp.length(), Bytes.toBytes(id), 0, id.length());
		AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(
				new RowKeyDistributorByHashPrefix.OneByteSimpleHash(10));
		return keyDistributor.getDistributedKey(row);
	}
	/**
	 * return a rowKey that format is:<ts.end><id>
	 * @throws IOException 
	 */
	public static byte[] PTDRowKey(String id,String ts) throws JsonMappingException, IOException{
		ObjectMapper mapper = new ObjectMapper();
		String end = ts;
		if(ts.indexOf("{") != -1) {
			Map<?, ?> t = mapper.readValue(ts, Map.class);
			String temp = t.get("end").toString();
			end = temp.replace("-", "").replace(" ", "").replace(":", "");
		}else {
			end = "3"+end;
		}
		id = formatString(id);
//		return makeInfoRowKey(id, end);
		return makeHaseRowKey(id, end);
	}
	private static String formatString(String in) {
		String out = in;
		while(out.length() < 17) {
			out = out + "x";
		}
		return out;
	}
}
