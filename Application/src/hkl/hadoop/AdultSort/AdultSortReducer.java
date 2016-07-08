package hkl.hadoop.AdultSort;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import hkl.hadoop.common.AdultInfoKey;

public class AdultSortReducer
	extends Reducer<AdultInfoKey, IntWritable, AdultInfoKey, IntWritable>
{
	private MultipleOutputs<AdultInfoKey, IntWritable> mos;
	
	// reduce outputKey
	private AdultInfoKey outputKey = new AdultInfoKey();
	// reduce outputValue
	private IntWritable result = new IntWritable();
	
	//called once
	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		mos = new MultipleOutputs<AdultInfoKey, IntWritable>(context);
	}
	
	public void reduce(AdultInfoKey key,Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException
	{
		String[] colums = key.toString().split(",");
		
		int sum = 0;
		Integer bHour = key.getHour();
		if(colums[0].equals("Male"))
		{
			for(IntWritable value : values)
			{
				if(bHour != key.getHour())
				{
					result.set(sum);
					outputKey.setAge(key.getAge().substring(5));
					outputKey.setHour(bHour);
					mos.write("Male", outputKey, result);
					sum = 0;
				}
				sum += value.get();
				bHour = key.getHour();
			}
			if(bHour == key.getHour())
			{
				outputKey.setAge(key.getAge().substring(5));
				outputKey.setHour(key.getHour());
				result.set(sum);
				mos.write("Male", outputKey, result);
			}
		}
		else
		{
			for(IntWritable value : values)
			{
				if(bHour != key.getHour())
				{
					result.set(sum);
					outputKey.setAge(key.getAge().substring(7));
					outputKey.setHour(bHour);
					mos.write("Female", outputKey, result);
					sum = 0;
				}
				sum += value.get();
				bHour = key.getHour();
			}
			if( bHour == key.getHour())
			{
				outputKey.setAge(key.getAge().substring(7));
				outputKey.setHour(key.getHour());
				result.set(sum);
				mos.write("Female", outputKey, result);
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		mos.close();
	}
}