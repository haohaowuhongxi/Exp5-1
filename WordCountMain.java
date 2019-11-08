package s51;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMain {
    public static void main (String[] args)throws Exception{
        //创建一个job和任务入口
        Job job = Job.getInstance(new Configuration());
        //main方法所在的class
        job.setJarByClass(WordCountMain.class);
        //指定job的mapper和输出的类型<k2,v2>
        job.setMapperClass(WordCountMapper.class);
        //k2的类型
        job.setMapOutputKeyClass(Text.class);
        //v2的类型
        job.setMapOutputValueClass(IntWritable.class);
        //指定job的mapper和输出的类型<k4,v4>
        job.setReducerClass(WordCountReducer.class);
        //k4的类型
        job.setOutputKeyClass(Text.class);
        //v4的类型
        job.setOutputValueClass(IntWritable.class);
        //指定job的输入和输出
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //执行job
        job.waitForCompletion(true);
    }
}
