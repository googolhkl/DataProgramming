package hkl.hadoop.AdultSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import hkl.hadoop.common.*;

public class GroupKeyPartitioner extends Partitioner<AdultInfoKey, IntWritable>
{
	@Override
	public int getPartition(AdultInfoKey key, IntWritable val, int numPartitions)
	{
		int hash = key.getAge().hashCode();
		int partition = hash % numPartitions;
		return partition;
	}
}
