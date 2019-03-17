package cn.windylee.hadoop.TopK;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopK extends Configured implements Tool {

    private static TreeMap<Integer, String> getTreeMap() {
        return new TreeMap<>((o1, o2) -> o2 - o1);
    }

    private static class TopKMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private IntWritable outputKey = new IntWritable();
        private Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String splits[] = value.toString().split("\t");
            outputKey.set(Integer.parseInt(splits[1]));
            outputValue.set(splits[0]);
            context.write(outputKey, outputValue);
        }
    }

    private static class TopKReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

        private TreeMap<Integer, String> treeMap = getTreeMap();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int topK = context.getConfiguration().getInt("topK", 5);
            for (Text value : values) {
                if (treeMap.size() < topK) treeMap.put(key.get(), value.toString());
                else {
                    if (key.get() > treeMap.lastKey()) {
                        treeMap.remove(treeMap.lastKey());
                        treeMap.put(key.get(), value.toString());
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : treeMap.entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("请指定输入输出路径");
            System.exit(-1);
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        getConf().setInt("topK", 10);
        FileSystem fileSystem = FileSystem.get(getConf());
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        Job job = Job.getInstance(getConf(), "topK");
        job.setJarByClass(TopK.class);
        job.setMapperClass(TopKMapper.class);
//        job.setCombinerClass(TopKReducer.class);
        job.setReducerClass(TopKReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean result = job.waitForCompletion(true);
        return result ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new TopK(), args);
    }
}
