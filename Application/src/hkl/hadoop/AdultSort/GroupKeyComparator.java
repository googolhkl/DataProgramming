package hkl.hadoop.AdultSort;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import hkl.hadoop.common.*;

public class GroupKeyComparator extends WritableComparator
{
	protected GroupKeyComparator()
	{
		super(AdultInfoKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		AdultInfoKey k1 = (AdultInfoKey)w1;
		AdultInfoKey k2 = (AdultInfoKey)w2;
		
		return k1.getAge().compareTo(k2.getAge());
	}
}
