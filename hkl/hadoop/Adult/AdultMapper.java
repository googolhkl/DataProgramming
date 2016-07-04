package hkl.hadoop.Adult;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import hkl.hadoop.Adult.AdultInformationParser;



public class AdultMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>
{
	
	private Text outputKey = new Text();
	private final static IntWritable outputValue = new IntWritable(1);
	
	public void map(LongWritable key, Text value,Context context)
		throws IOException, InterruptedException
	{
		AdultInformationParser parser = new AdultInformationParser(value);
		
		outputKey.set(parser.getRace()+ "," + parser.getSex() + "," + parser.getHoursPerWeek());
		if(parser.getPay().equals("<=50K"))
		{
			context.write(outputKey, outputValue);
		}
	}
}
