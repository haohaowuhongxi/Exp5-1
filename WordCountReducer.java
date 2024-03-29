package s51;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
    @Override
    protected void reduce(Text k3, Iterable<IntWritable> v3, Context context)
            throws IOException, InterruptedException{
        //对v3求和
        int total = 0;
        for (IntWritable v:v3) {
            total += v.get();
        }
        //输出：k4 单词   v4 频率
        context.write(k3, new IntWritable(total));
    }
}
