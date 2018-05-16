import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;

import com.fasterxml.jackson.databind.JsonNode;

public class Test {
	public static void main(String[] args) throws Exception{
		String rowName = "URL";
		
		Configuration conf = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("pkg"));
		TableDescriptor tableDescriptor = table.getDescriptor();
		Set<byte[]> columnName = tableDescriptor.getColumnFamilyNames();
		Iterator<byte[]> iterator = columnName.iterator();
		while (iterator.hasNext()) {
			String string = new String(iterator.next());
			System.out.println(string + ";");
			if (rowName.equalsIgnoreCase(string)) {
				System.out.println("break");
				break;
			}
		}
		System.exit(0); 
    } 
	public static String[] getKeyValue(Map<?, ?> pkg,String key) {
		String result[]= {"",""};
		if (pkg.containsKey(key)) {
			result[0] = key;
			result[1] = pkg.get(key).toString();
		}
		return result;
	}
	public static void jsonLeaf(JsonNode node)
    {
        if (node.isValueNode() || node.isContainerNode())
        {
        	String key = node.fieldNames().next();
        	String value = node.findValue(key).toString();
            System.out.println(key + " " +value);
            return;
        }

        if (node.isObject())
        {
            Iterator<Entry<String, JsonNode>> it = node.fields();
            while (it.hasNext())
            {
                Entry<String, JsonNode> entry = it.next();
                jsonLeaf(entry.getValue());
            }
        }

        if (node.isArray())
        {
            Iterator<JsonNode> it = node.iterator();
            while (it.hasNext())
            {
                jsonLeaf(it.next());
            }
        }
    }
}
