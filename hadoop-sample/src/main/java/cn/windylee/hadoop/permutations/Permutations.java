package cn.windylee.hadoop.permutations;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Permutations extends Configured implements Tool {

    private static class PermutationsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits1 = value.toString().split("\\|");
            String subPermutation = splits1[0];
            String[] restDigits = splits1[1].split(",");
            for (int i = 0; i < restDigits.length; ++i) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < restDigits.length; ++j) {
                    if (j != i) sb.append(restDigits[j]).append(",");
                }
                if (sb.length() > 0)
                    sb.deleteCharAt(sb.length() - 1);
                context.write(new Text(subPermutation + restDigits[i] + "|" + sb.toString()), NullWritable.get());
            }
        }
    }

    private static class PermutationsReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("请指定文件路径");
            System.exit(-1);
        }
        for (int i = 1; i < 6; ++i) {
            Path inputPath = new Path(args[0] + (i - 1));
            Path outputPath = new Path(args[0] + i);
            Job job = Job.getInstance(getConf(), "permutations");
            job.setJarByClass(Permutations.class);
            job.setMapperClass(PermutationsMapper.class);
            job.setReducerClass(PermutationsReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.waitForCompletion(true);
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Permutations(), args);
    }
}
