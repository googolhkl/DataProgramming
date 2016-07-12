package hkl.hadoop.AdultSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AdultPartialSortMapFileCreator extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
        int res = ToolRunner.run(new Configuration(), new AdultPartialSortMapFileCreator(), args);
        System.out.println("MR-Job Result : " + res);
	}

    public int run(String[] args) throws Exception
    {
        JobConf conf = new JobConf(AdultPartialSortMapFileCreator.class);
        conf.setJobName("AdultPartialSortMapFileCreator");

        //입출력 경로설정
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //입력 데이터를 시퀀스파일로 설정
        conf.setInputFormat(SequenceFileInputFormat.class);
        //출력 데이터를 맵파일로 설정
        conf.setOutputFormat(MapFileOutputFormat.class);
        //출력 데이터의 키를 근무시간(IntWritable)로 설정
        conf.setOutputKeyClass(IntWritable.class);

        //시퀀스파일 압출 포맷 설정
        SequenceFileOutputFormat.setCompressOutput(conf, true);
        SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);

        JobClient.runJob(conf);
        return 0;
    }
}
