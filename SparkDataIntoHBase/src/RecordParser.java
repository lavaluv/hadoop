import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RecordParser implements Serializable{
	private static final long serialVersionUID = 1L;
	public static Map<?, ?> parse(String line) throws IOException,InterruptedException{
		//to parse json string as <key:value>
		Map<?, ?> pkg = null;
		if(line != "") {
			ObjectMapper mapper = new ObjectMapper();
			pkg = mapper.readValue(line, Map.class);
		}
		return pkg;
	}
	public static String[] getKeyValue(Map<?, ?> pkg,String key) {
		String[] result= {};
		if (pkg.containsKey(key)) {
			result[0] = key;
			result[1] = pkg.get(key).toString();
		}
		return result;
	}
	public static ArrayList<Map<?, ?>> ptd_parse(String raw)throws IOException,InterruptedException{
        ArrayList<Map<?, ?>> out = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node = mapper.readTree(raw);
		Iterator<Entry<String, JsonNode>> it = node.fields();  
        while (it.hasNext())  
        {  
            Entry<String, JsonNode> entry = it.next(); 
            if (entry.getKey().equals("data")) {
            	Iterator<JsonNode> array = entry.getValue().iterator(); 
            	while(array.hasNext()) {
            		Map<?, ?> ptd_data = RecordParser.parse(array.next().toString());
            		out.add(ptd_data);
            	}
            	break;
			}
        }  
		return out;
	}
    public static void jsonLeaf(JsonNode node)  
    {  
      
        if (node.isObject())  
        {  
        	System.out.println("object");
            Iterator<Entry<String, JsonNode>> it = node.fields();  
            while (it.hasNext())  
            {  
                Entry<String, JsonNode> entry = it.next();  
                jsonLeaf(entry.getValue());  
            }  
        }  
        else if (node.isArray())  
        {  
        	System.out.println("array");
            Iterator<JsonNode> it = node.iterator();  
            while (it.hasNext())  
            {  
                jsonLeaf(it.next());  
            }  
        }  
        else if (node.isValueNode() || node.isContainerNode())  
        {  
            System.out.printf("[%s]\n",node.toString());  
            return;  
        }  
    } 
}
