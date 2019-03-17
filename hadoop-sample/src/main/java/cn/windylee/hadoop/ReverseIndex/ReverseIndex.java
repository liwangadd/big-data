package cn.windylee.hadoop.ReverseIndex;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

public class ReverseIndex extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(ReverseIndex.class);

    private static class ReverseWordMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = FilenameUtils.getBaseName(split.getPath().toString());
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().toLowerCase();
                word = word + ":" + fileName;
                context.write(new Text(word), new Text("1"));
            }
        }
    }

    private static class ReverseWordCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (Text value : values) {
                ++sum;
            }
            String[] splits = key.toString().split(":");
            context.write(new Text(splits[0]), new Text(splits[1] + ":" + sum));
        }
    }

    private static class ReverseWordReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value).append(" ");
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            log.error("请指定输入文件和输出文件路径");
            System.exit(-1);
        }
        Configuration conf = getConf();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "InverseWord");
        job.setJarByClass(ReverseIndex.class);
        job.setMapperClass(ReverseWordMapper.class);
        job.setCombinerClass(ReverseWordCombiner.class);
        job.setReducerClass(ReverseWordReducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ReverseIndex(), args);
    }

}
