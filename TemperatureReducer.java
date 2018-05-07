import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureReducer
extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	@Override
	public void reduce(Text key,Iterable<IntWritable> value,
		Context context)
		throws IOException,InterruptedException{
			//Do reduce
			context.write(key,new IntWritable(1));
		}
}