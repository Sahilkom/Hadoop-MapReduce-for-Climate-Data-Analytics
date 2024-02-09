import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Word_Count_Task {

  // Mapper Class
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // Map method processes each input record (line) and emits key-value pairs
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Tokenize the input line into words
      StringTokenizer itr = new StringTokenizer(value.toString());

      // Emit key-value pairs for each word in the line
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one); // Emit (word, 1)
      }
    }
  }

  // Reducer Class
  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    // Reduce method processes the intermediate key-value pairs and emits final results
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;

      // Sum up the counts for the current word
      for (IntWritable val : values) {
        sum += val.get();
      }

      result.set(sum);
      context.write(key, result); // Emit (word, total count)
    }
  }

  // Main method
  public static void main(String[] args) throws Exception {
    // Create a Hadoop Configuration
    Configuration conf = new Configuration();

    // Create a new Hadoop job and set its properties
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Word_Count_Task.class);

    // Set Mapper and Reducer classes
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class); // Optional combiner for local aggregation
    job.setReducerClass(IntSumReducer.class);

    // Set output key and value classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Set input and output paths from command-line arguments
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Submit the job and wait for completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

