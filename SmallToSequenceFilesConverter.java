import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmallToSequenceFilesConverter extends Configured
implements Tool{
	static class SequnceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{
		private Text fileNameKey;
			
		@Override
		protected void setup(Context context)throws IOException,InterruptedException{
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit)split).getPath();
			fileNameKey = new Text(path.toString());
		}
			
		@Override
		protected void map(NullWritable key,BytesWritable value,Context context)
			throws IOException,InterruptedException{
			context.write(fileNameKey, value);
		}
	}
	public static Job parseInputAndOutput(Tool tool,Configuration conf,String[] args)
	throws IOException{
		if(args.length != 2) {
			System.err.println("Usage:SmallToSequenceFilesConverter <input path><output path>");
			System.exit(-1);
		}
		Job job = Job.getInstance();
		job.setJarByClass(tool.getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job;
	}
	@Override
	public int run(String[] args)throws IOException, ClassNotFoundException, InterruptedException{
		Job job = parseInputAndOutput(this,getConf(),args);
		if(job == null) {
			return -1;
		}
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setMapperClass(SequnceFileMapper.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new SmallToSequenceFilesConverter(), args);
		System.exit(exitCode);
	}
}
