import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureMapper 
extends Mapper<LongWritable,Text,Text,IntWritable>{
	@Override
	public void map(LongWritable key,Text value,Context context)
	throws IOException,InterruptedException{
		//Do map
		//String line = value.toString();
		//use Context to write
		context.write(new Text(value),new IntWritable(0));
	}
}