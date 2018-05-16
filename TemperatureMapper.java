import java.io.IOException;
//import java.util.Iterator;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TemperatureMapper 
extends Mapper<LongWritable,Text,Text,Text>{
	@Override
	public void map(LongWritable key,Text value,Context context)
	throws IOException,InterruptedException{
		//Do map
		String line = value.toString();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node = mapper.readTree(line);
//		Iterator<JsonNode> iterator = node.get("cp").iterator();
//		String ip = "";
//		while (iterator.hasNext()) {
//			JsonNode jsonNode = (JsonNode) iterator.next();
//			ip += jsonNode.get("ip");
//		}
		//use Context to write
		context.write(new Text(node.get("dst").get("city").toString()),
				new Text("ok"));
	}
}