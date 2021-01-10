package cn.keats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PhoneFlowTest {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.output.textoutputformat.separator", "\t");
        Job job = Job.getInstance(conf);

        job.setJarByClass(PhoneFlowTest.class);
        job.setMapperClass(PhoneFlowTest.phoneFlowMap.class);
        job.setReducerClass(PhoneFlowTest.phoneFlowReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneFlowWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneFlowWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

    static class phoneFlowMap extends Mapper<LongWritable, Text, Text, PhoneFlowWritable> {
        Text outKey = new Text();
        PhoneFlowWritable outValue = new PhoneFlowWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] strings = line.split(" ");

            outKey.set(strings[1]);
            outValue.setUpFlow(Long.parseLong(strings[4]));
            outValue.setDownFlow(Long.parseLong(strings[5]));

            context.write(outKey, outValue);
        }
    }

    static class phoneFlowReduce extends Reducer<Text, PhoneFlowWritable, Text, PhoneFlowWritable> {
        PhoneFlowWritable outValue = new PhoneFlowWritable();
        @Override
        protected void reduce(Text key, Iterable<PhoneFlowWritable> values, Context context) throws IOException, InterruptedException {
            long sumFlow = 0;
            long upFlow = 0;
            long downFlow = 0;
            for (PhoneFlowWritable value : values) {
                sumFlow += value.getUpFlow();
                sumFlow += value.getDownFlow();
                upFlow += value.getUpFlow();
                downFlow += value.getDownFlow();
            }
            outValue.setUpFlow(upFlow);
            outValue.setDownFlow(downFlow);
            outValue.setSumFlow(sumFlow);

            context.write(key, outValue);
        }
    }



}
