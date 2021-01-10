package cn.keats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyWordCount {

    /**
     * 主函数，驱动类代码
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取 job 实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 配置驱动 jar 、 map、reduce jar
        job.setJarByClass(MyWordCount.class);
        job.setMapperClass(wordCountMap.class);
        job.setReducerClass(wordCountReduce.class);

        // 配置 map 阶段输出类型，reduce 阶段输入类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 输入和输出转换成文件，通过 main 方法参数传递
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 等待结果
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

    /**
     * Map 阶段代码
     *  1. 继承 Mapper 接口
     *  2. Mapper 接口的四个参数分别对应：输入参数的 key, 输入参数的 value, 输出参数的 key, 输出参数的 value
     *  3. MapReduce 框架会将输入文本按行读取，每一行调用一次 map 方法。而 map 方法中就是我们对文件内容所进行的操作
     *  4. 最后通过 context 上下文对象，将结果写出
     */
    static class wordCountMap extends Mapper<LongWritable, Text, Text, IntWritable>{
        Text k = new Text();
        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");

            for (String word : words) {
                k.set(word);
                context.write(k, v);
            }
        }
    }

    /**
     * Reduce 阶段代码
     *  1. 继承 Reduce 接口
     *  2. Reduce 接口参数和 Map 含义一致， 一般情况下 Reduce 的入参就是 Map 的出参
     */
    static class wordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        IntWritable v = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (IntWritable value : values) {
                num += value.get();
            }
            v.set(num);
            context.write(key, v);
        }
    }
}
