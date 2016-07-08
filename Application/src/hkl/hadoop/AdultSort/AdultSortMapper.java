package hkl.hadoop.AdultSort;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import hkl.hadoop.common.AdultInformationParser;
import hkl.hadoop.common.AdultOption;
import hkl.hadoop.common.AdultInfoKey;

public class AdultSortMapper
	extends Mapper<LongWritable, Text, AdultInfoKey, IntWritable>
{
	private AdultInfoKey outputKey = new AdultInfoKey();
	private final static IntWritable outputValue = new IntWritable(1);
	
	public void map(LongWritable key, Text value,Context context)
		throws IOException, InterruptedException
	{
		AdultInformationParser parser = new AdultInformationParser(value);
		//outputKey 설정
		int temp = parser.getAge();
		temp /= 10;
		temp *= 10;
		parser.setAge(temp);

		// income >= 50K
		if(parser.getPay().equals("<=50K"))
		{
			if(parser.getSex().equals("Male"))
			{
				if( (parser.getAge() >= 20) && (parser.getAge() <50) ) //20~49
				{
					outputKey.setAge("Male," + parser.getAge());
					outputKey.setHour(parser.getHoursPerWeek());
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
			else if(parser.getSex().equals("Female"))
			{
				if( (parser.getAge() >= 20) && (parser.getAge() <50) ) //20~49
				{
					outputKey.setAge("Female," + parser.getAge());
					outputKey.setHour(parser.getHoursPerWeek());
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
		}
	} //map
}