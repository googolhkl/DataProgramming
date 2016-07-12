package hkl.hadoop.AdultSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AdultPartialSortSearch extends Configured implements Tool
{
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new AdultPartialSortSearch(), args);
        System.out.println("MR-Job Result : " + res);
    }

    public int run(String[] args) throws Exception
    {
        Path path = new Path(args[0]);
        FileSystem fs = path.getFileSystem(getConf()); // 로컬파일or HDFS 사용시 반드시 FileSystem사용.
        
        Reader[] readers = MapFileOutputFormat.getReaders(fs, path, getConf()); // 맵파일에 저장된 데이터 목록을 반환
        
        IntWritable key = new IntWritable();
        key.set(Integer.parseInt(args[1])); // 사용자가 입력한 근무시간을 키로 설정

        Text value = new Text();

        // 해시 파티너를 생성하는 이유는 맵파일이 해시 파티셔너로 파티셔닝 됐기 때문
        Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
        // 특정 키에 대한 파티션 번호를 반환
        Reader reader = readers[partitioner.getPartition(key, value, readers.length)];

        // 특정 키에 대한 값을 검색. 데이터 목록중에 첫 번째 값이다.
        Writable entry = reader.get(key, value);
        if(entry == null)
        {
            System.out.println("The request key was not found.");   
        }

        IntWritable nextKey = new IntWritable();
        // 맵파일에 있는 모든 데이터를 조회(사용자가 입력한 키와 같은 키만 출력)
        do
        {
            System.out.println(value.toString());
        }while(reader.next(nextKey, value) && key.equals(nextKey));
        return 0;
    }
}
