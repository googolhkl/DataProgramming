package hkl.hadoop.Adult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AdultDriver  
{
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		if(args.length != 2)
		{
			System.err.println("Usage: AdultDriver <input> <output>");
			System.exit(2);
		}
		
		Job job = new Job(conf,"AdultDriver");
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setJarByClass(AdultDriver.class);
		
		System.out.println("Before Mapper Execute");
		job.setMapperClass(AdultMapper.class);
		System.out.println("After Mapper Execute");

		job.setReducerClass(AdultReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
	}
}
