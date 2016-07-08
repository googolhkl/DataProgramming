package hkl.hadoop.AdultSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import hkl.hadoop.common.*;

public class AdultSort  extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new AdultSort() ,args);
		System.out.println("MapReduce Job result = " + res);

	}
	public int run(String[] args) throws Exception
	{
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		
		if(otherArgs.length != 2)
		{
			System.out.println("Usage : AdultSort <input> <output>");
			System.exit(2);
		}
		Job job = new Job(getConf(), "AdultSort");
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setJarByClass(AdultSort.class);
		job.setPartitionerClass(GroupKeyPartitioner.class);
		job.setGroupingComparatorClass(GroupKeyComparator.class);
		job.setSortComparatorClass(AdultKeyComparator.class);
		
		job.setMapperClass(AdultSortMapper.class);
		job.setReducerClass(AdultSortReducer.class);
		
		job.setMapOutputKeyClass(AdultInfoKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(AdultInfoKey.class);
		job.setOutputValueClass(IntWritable.class);
		
		MultipleOutputs.addNamedOutput(job, "Male", TextOutputFormat.class, AdultInfoKey.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "Female", TextOutputFormat.class, AdultInfoKey.class, IntWritable.class);
		job.waitForCompletion(true);

		return 0;
	}
}