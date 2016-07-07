package hkl.hadoop.Adult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AdultOptionCountDriver  extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new AdultOptionCountDriver() ,args);
		System.out.println("MapReduce Job result = " + res);

	}
	public int run(String[] args) throws Exception
	{
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		
		if(otherArgs.length != 2)
		{
			System.out.println("Usage : AdultOptionCountDriver <input> <output>");
			System.exit(2);
		}
		
		Job job = new Job(getConf(), "AdultOptionCountDriver");
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setJarByClass(AdultOptionCountDriver.class);
		
		job.setMapperClass(AdultOptionCountMapper.class);

		job.setReducerClass(AdultOptionCountReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		MultipleOutputs.addNamedOutput(job, "Male", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "Female", TextOutputFormat.class, Text.class, IntWritable.class);
		
		job.waitForCompletion(true);
		return 0;
	}
}
