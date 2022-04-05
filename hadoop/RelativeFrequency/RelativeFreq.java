import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RelativeFreq {

    public static class PairsRelativeOccurrenceMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
        private WordPair wordPair = new WordPair();
        private IntWritable ONE = new IntWritable(1);
        private IntWritable totalCount = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int neighbors = context.getConfiguration().getInt("neighbors", 2);
            String[] tokens = value.toString().split("\\s+");          // split the words using spaces
            if (tokens.length > 1) {
                for (int i = 0; i < tokens.length; i++) {
                        tokens[i] = tokens[i].replaceAll("\\W+","");   // remove all non-word characters

                        if(tokens[i].equals("")){
                            continue;
                        }

                        wordPair.setWord(tokens[i]);

                        int start = (i - neighbors < 0) ? 0 : i - neighbors;
                        int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
                        for (int j = start; j <= end; j++) {
                            if (j == i) continue;
                            wordPair.setNeighbor(tokens[j].replaceAll("\\W",""));
                            context.write(wordPair, ONE);
                        }
                        wordPair.setNeighbor("*");
                        totalCount.set(end - start);
                        context.write(wordPair, totalCount);
                }
            }
        }
    }



    public class WordPairPartitioner extends Partitioner<WordPair,IntWritable> {

        @Override
        public int getPartition(WordPair wordPair, IntWritable intWritable, int numPartitions) {
            return wordPair.getWord().hashCode() % numPartitions;
        }
    }

    public static class PairsRelativeOccurrenceReducer extends Reducer<WordPair, IntWritable, WordPair, DoubleWritable> {
        private DoubleWritable totalCount = new DoubleWritable();
        private DoubleWritable relativeCount = new DoubleWritable();
        private Text currentWord = new Text("NOT_SET");
        private Text flag = new Text("*");

        @Override
        protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            if (key.getNeighbor().equals(flag)) {          
                if (key.getWord().equals(currentWord)) {   
                                                           
                    totalCount.set(totalCount.get() + getTotalCount(values));
                } else {                                   
                    currentWord.set(key.getWord());
                    totalCount.set(0);
                    totalCount.set(getTotalCount(values));
                }
            } else {                                       
                int count = getTotalCount(values);
                relativeCount.set((double) count / totalCount.get());
                context.write(key, relativeCount);
            }
        }
      private int getTotalCount(Iterable<IntWritable> values) {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            return count;
        }


    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
          System.err.println("Usage: relativefreq <in> <out>");
          System.exit(2);
        }
        Job job = new Job(conf, "Wordpair Relative Frequency");
        job.setJarByClass(RelativeFreq.class);
        job.setMapperClass(PairsRelativeOccurrenceMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(PairsRelativeOccurrenceReducer.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}