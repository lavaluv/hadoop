import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HBaseImporter extends Configured implements Tool{
	static class HBaseMapper<K> extends Mapper<LongWritable, Text, K, Put>{
		private RecordParser parser = new RecordParser();
		
		@Override
		public void map(LongWritable key,Text input,Context context)
				throws IOException,InterruptedException{
			Map<?, ?> pkg = parser.parse(input.toString());
			if(!pkg.isEmpty()) {
				byte[] rowKey = 
						RowKeyConverter.makeInfoRowKey(pkg.get("id").toString(),
						pkg.get("ts").toString());
				Put put = new Put(rowKey);
				for(Map.Entry<?, ?> entry : pkg.entrySet()) {
					byte[] column = Bytes.toBytes(entry.getKey().toString());
					byte[] qualifier = Bytes.toBytes(String.valueOf(1));
					byte[] value = Bytes.toBytes(entry.getValue().toString());
					put.addColumn(column,qualifier,value);
				}
				context.write(null, put);
			}
		}
	}
	@Override
	public int run(String[] args)throws Exception{
		if (args.length != 1) {
			System.err.println("Usage: HBaseImporter <input>");
			return -1;
		}
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.fileinputformat.input.dir.recursive","true"); 
		Job job = Job.getInstance(conf, getClass().getSimpleName());
		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,"pkg");
		job.setMapperClass(HBaseMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TableOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception{
		int exit = ToolRunner.run(HBaseConfiguration.create(),
				new HBaseImporter(),args);
		System.exit(exit);
	}
}
