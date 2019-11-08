package s51;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper <LongWritable, Text, Text, IntWritable>  {
    @Override
    protected void map(LongWritable key, Text value1, Context context)
            throws IOException, InterruptedException {
        //数据
        String data = value1.toString();
        //分词
        String[] words = data.split(" ");
        //输出k2 v2
        for(String w:words){
            context.write(new Text(w), new IntWritable(1));
        }
    }
}
