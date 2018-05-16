import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureReducer
extends Reducer<Text,Text,Text,Integer>{
	
	@Override
	public void reduce(Text key,Iterable<Text> values,
		Context context)
		throws IOException,InterruptedException{
			//Do reduce
			Map<Text, Integer> map = new HashMap<Text,Integer>(); 
			Integer temp = 1;
			for(@SuppressWarnings("unused") Text value:values) {
				//context.write(key,new Text(value));
				if (map.containsKey(key)) {
					temp = map.get(key);
					map.remove(key);
					map.put(key, temp + 1);
				}
				else {
					map.put(key,temp);
				}
			}
			context.write(key, map.get(key));
		}
}