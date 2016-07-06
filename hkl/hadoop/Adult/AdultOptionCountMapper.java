package hkl.hadoop.Adult;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import hkl.hadoop.Adult.AdultInformationParser;



public class AdultOptionCountMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>
{
	// operating type
	private String workType;
	
	private Text outputKey = new Text();
	private final static IntWritable outputValue = new IntWritable(1);
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		workType = context.getConfiguration().get("workType");
	}
	
	public void map(LongWritable key, Text value,Context context)
		throws IOException, InterruptedException
	{
		AdultInformationParser parser = new AdultInformationParser(value);
		
        //outputKey 설정
		int temp = parser.getAge();
		temp /= 10;
		temp *= 10;
		parser.setAge(temp);
		outputKey.set(parser.getAge()+ "," + parser.getRace()+ "," + parser.getSex());

		// income >= 50K
		if(parser.getPay().equals("<=50K"))
		{
			//context.getCounter(AdultOption.age_between_70_100).increment(516);
			if(workType.equals("age"))
			{
				if( (parser.getAge() >= 20) && (parser.getAge() <50) ) //20~49
				{
					context.write(outputKey, outputValue);
				}
				else if( (parser.getAge() >=10) &&(parser.getAge() <20) ) //10~19
				{
					context.getCounter(AdultOption.age_between_10_20).increment(1);
				}
				else if( (parser.getAge() >= 50) && (parser.getAge() < 70 )) //50~69
				{
					context.getCounter(AdultOption.age_between_50_70).increment(1);
				}
				else if( (parser.getAge() >= 70) && (parser.getAge() < 100) ) //70~99
				{
					context.getCounter(AdultOption.age_between_70_100).increment(1);
				}			
			}
			else if(workType.equals("hour"))
			{
				if( (parser.getHoursPerWeek() <10) ) // ~9
				{
					context.getCounter(AdultOption.hour_week_between_0_10).increment(1);
				}
				else if( (parser.getHoursPerWeek() >=10) && (parser.getHoursPerWeek() <20) )
				{
					context.getCounter(AdultOption.hour_week_between_10_20).increment(1);
				}
				else if( (parser.getHoursPerWeek() >=20) && (parser.getHoursPerWeek() <30))
				{
					context.getCounter(AdultOption.hour_week_between_20_30).increment(1);
				}
				else if( (parser.getHoursPerWeek() >=30) && (parser.getHoursPerWeek() <50))
				{
					context.write(outputKey, outputValue);
				}
				else if( (parser.getHoursPerWeek() >=50) && (parser.getHoursPerWeek() <70))
				{
					context.getCounter(AdultOption.hour_week_between_50_70).increment(1);
				}
				else if( (parser.getHoursPerWeek() >=70) && (parser.getHoursPerWeek() <100))
				{
					context.getCounter(AdultOption.hour_week_between_70_100).increment(1);
				}
			}
		}
		
	} //map
}