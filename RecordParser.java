import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RecordParser implements Serializable{
	private static final long serialVersionUID = 1L;
	public static Map<?, ?> parse(String line) throws IOException,InterruptedException{
		//to parse json string as <key:value>
		ObjectMapper mapper = new ObjectMapper();
		Map<?, ?> pkg = mapper.readValue(line, Map.class);
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
}
