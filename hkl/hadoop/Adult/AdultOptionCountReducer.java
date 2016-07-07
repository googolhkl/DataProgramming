package hkl.hadoop.Adult;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class AdultOptionCountReducer 
	extends Reducer<Text, IntWritable, Text, IntWritable>
{
	private MultipleOutputs<Text, IntWritable> mos;
	
	// reduce outputKey
	private Text outputKey = new Text();
	// reduce outputValue
	private IntWritable result = new IntWritable();
	
	//called once
	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}
	
	public void reduce(Text key,Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException
	{
		String[] colums = key.toString().split(",");
		
		// Age,Race,Sex
		outputKey.set(colums[1] + "," + colums[2] + "," + colums[3]);
		
		// Male
		if(colums[0].equals("M"))
		{
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			result.set(sum);
			mos.write("Male", outputKey, result);
		}
		else // Female
		{
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			result.set(sum);
			mos.write("Female", outputKey, result);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		mos.close();
	}
}
