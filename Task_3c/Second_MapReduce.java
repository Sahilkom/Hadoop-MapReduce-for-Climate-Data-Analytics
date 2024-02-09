import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Second_MapReduce {

    // Mapper Class
    public static class Second_TemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        // Map method processes each input record and emits key-value pairs
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Splitting the input line into tokens using whitespace
            String[] tokens = value.toString().split("\\s+");
            
            // Emitting key-value pair (date, temperature difference)
            context.write(new Text(tokens[0]), new Text(tokens[2]));
        }
    }

    // Reducer Class
    public static class Second_TemperatureReducer extends Reducer<Text, Text, Text, Text> {

        // Reduce method processes the intermediate key-value pairs and emits final results
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;
            
            // Iterating through temperature differences for each date
            for (Text val : values) {
                double diff = Double.parseDouble(val.toString());
                sum += diff;
                
                // Counting non-zero differences
                if (diff != 0) {
                    count++;
                }
            }
            
            // Calculating average temperature difference and emitting the result
            if (count > 0) {
                double average = sum / count;
                context.write(key, new Text(String.valueOf(average)));
            } else {
                context.write(key, new Text("No non-zero differences found"));
            }
        }
    }

    // Main method
    public static void main(String[] args) throws Exception {
        // Create a Hadoop Configuration
        Configuration conf = new Configuration();

        // Create a new Hadoop job and set its properties
        Job job = Job.getInstance(conf, "Second MapReduce class Analysis");
        job.setJarByClass(Second_MapReduce.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(Second_TemperatureMapper.class);
        job.setReducerClass(Second_TemperatureReducer.class);

        // Set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths from command-line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

