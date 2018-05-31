import java.util.ArrayList;
import java.util.Map;


public class Tests {
	public static void main(String[] args) throws Exception{
			String in = "{\"count\":\"128\",\"data\":[{\"a1\":\"1\"},{\"a2\":\"2\"},{\"a3\":\"3\"}]}";
			ArrayList<Map<?, ?>> out = RecordParser.ptd_parse(in);
			for( Map<?, ?> ptd:out) {
				for(Map.Entry<?, ?> entry:ptd.entrySet()) {
					System.out.println(entry.getKey()+":"+entry.getValue());
				}
			}
	}
}

