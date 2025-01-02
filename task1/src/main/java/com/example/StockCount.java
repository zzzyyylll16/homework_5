package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private Text stockCode = new Text();
        private IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Split the line by comma but only consider the last part as stock code
            String[] tokens = line.split(",");
            if (tokens.length > 3) {
                stockCode.set(tokens[tokens.length - 1].trim());
                context.write(stockCode, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private List<StockCountPair> stockCounts = new ArrayList<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            stockCounts.add(new StockCountPair(key.toString(), sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (stockCounts != null && !stockCounts.isEmpty()) {
                // Sort the list in descending order of count
                Collections.sort(stockCounts, new Comparator<StockCountPair>() {
                    public int compare(StockCountPair o1, StockCountPair o2) {
                        return Integer.compare(o2.count, o1.count);
                    }
                });

                // Write the sorted results to the context with rank
                int rank = 1;
                for (StockCountPair pair : stockCounts) {
                    context.write(new Text(rank + "：" + pair.stockCode), new IntWritable(pair.count));
                    rank++;
                }
            }
        }
    }

    public static class StockCountPair {
        String stockCode;
        int count;

        StockCountPair(String stockCode, int count) {
            this.stockCode = stockCode;
            this.count = count;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stock count");
        job.setJarByClass(StockCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
