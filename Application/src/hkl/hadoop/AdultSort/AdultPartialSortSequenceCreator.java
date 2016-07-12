package hkl.hadoop.AdultSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;

import org.apache.hadoop.mapred.*;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hkl.hadoop.common.AdultInformationParser;
import java.io.IOException;

public class AdultPartialSortSequenceCreator extends Configured implements Tool
{


    static class HourMapper
        extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
        private IntWritable outputKey = new IntWritable();

        public void map(LongWritable key, Text value, 
            OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
        {
            AdultInformationParser parser = new AdultInformationParser(value);
            outputKey.set(parser.getHoursPerWeek());
            output.collect(outputKey,value);
        } // map
    } // HourMapper

    public int run(String[] args) throws Exception
    {
        JobConf conf = new JobConf(AdultPartialSortSequenceCreator.class);
        conf.setJobName("AdultPartialSortSequenceCreator");
        
        conf.setMapperClass(HourMapper.class);
        conf.setNumReduceTasks(0);

        // set I/O path
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        SequenceFileOutputFormat.setCompressOutput(conf, true);
        SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
        JobClient.runJob(conf);
        return 0;
    }
    
	public static void main(String[] args) throws Exception
	{
        int res = ToolRunner.run(new Configuration(), new AdultPartialSortSequenceCreator(), args);
        System.out.println("MR-Job Result : " + res);
	}
}
