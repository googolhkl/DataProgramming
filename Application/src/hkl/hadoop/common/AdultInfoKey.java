package hkl.hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AdultInfoKey implements WritableComparable<AdultInfoKey>
{
	private String age;
	private Integer hour;
	
	public AdultInfoKey(){}
	public AdultInfoKey(String age, int hour)
	{
		this.age = age;
		this.hour = hour;
	}
	
	// getters
	public String getAge() { return age;}
	public int getHour() { return hour;}
	
	// setters 
	public void setAge(String age) { this.age = age;}
	public void setHour(int hour) { this.hour = hour;}
	
	@Override
	public String toString()
	{
		return (new StringBuilder()).append(age).append(",").append(hour).toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		age = WritableUtils.readString(in);
		hour = in.readInt();
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeString(out, age);
		out.writeInt(hour);
	}
	
	@Override
	public int compareTo(AdultInfoKey key)
	{
		int result = age.compareTo(key.age);
		
		if( result == 0)
		{
			result = hour.compareTo(key.hour);
		}
		return result;
	}
}
