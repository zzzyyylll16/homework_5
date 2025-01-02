package task2.example;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader; // 添加这个导入
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem; // 添加这个导入
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopWords {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords;

        public TokenizerMapper() {
            stopWords = new HashSet<>();
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path stopWordsPath = new Path(conf.get("stopwords.file"));
            FileSystem fs = FileSystem.get(conf);
            // Load stop words from HDFS
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)))) {
                for (String line : reader.lines().toArray(String[]::new)) { // 将 Stream<String> 转换为 String[]
                    stopWords.add(line.trim().toLowerCase());
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length > 1) {
                String headline = tokens[1].trim();
                headline = headline.replaceAll("[^a-zA-Z0-9\\s]", "");
                StringTokenizer itr = new StringTokenizer(headline);
                while (itr.hasMoreTokens()) {
                    String str = itr.nextToken().toLowerCase();
                    if (!stopWords.contains(str)) {
                        word.set(str);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private List<WordCountPair> wordCounts = new ArrayList<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            wordCounts.add(new WordCountPair(key.toString(), sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(wordCounts, (a, b) -> b.count - a.count);
            for (int i = 0; i < Math.min(100, wordCounts.size()); i++) {
                context.write(new Text("排名：" + (i + 1) + "：" + wordCounts.get(i).word + "，" + wordCounts.get(i).count), null);
            }
        }
    }

    public static class WordCountPair {
        String word;
        int count;

        WordCountPair(String word, int count) {
            this.word = word;
            this.count = count;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Set the path to the stop words file in HDFS
        conf.set("stopwords.file", "input/stop-word-list.txt");
        Job job = Job.getInstance(conf, "top words");
        job.setJarByClass(TopWords.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
