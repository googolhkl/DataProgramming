package hkl.hadoop.AdultSort;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import hkl.hadoop.common.*;

public class AdultKeyComparator extends WritableComparator
{
	protected AdultKeyComparator()
	{
		super(AdultInfoKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		AdultInfoKey k1 = (AdultInfoKey)w1;
		AdultInfoKey k2 = (AdultInfoKey)w2;
		
		// compare
		int cmp = k1.getAge().compareTo(k2.getAge());
		if(cmp != 0)
		{
			return cmp;
		}
		
		return k1.getHour() == k2.getHour() ? 0 :
			(k1.getHour() < k2.getHour() ? -1 : 1);
	}
}