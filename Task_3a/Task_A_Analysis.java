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

public class Task_A_Analysis {

    // Mapper Class
    public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        // Map method processes each input record and emits key-value pairs
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Splitting the input line into tokens using ","
            String[] tokens = value.toString().split(",");
            
            // Extracting temperature values
            String tmax = tokens[0];
            String tmin = tokens[1];
            
            // Emitting key-value pair (tmax, tmin)
            context.write(new Text(tmax), new Text(tmin));
        }
    }

    // Reducer Class
    public static class TemperatureReducer extends Reducer<Text, Text, Text, Text> {
        int c = 0;

        // Reduce method processes the intermediate key-value pairs and emits final results
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                // Converting the temperature values from string to double
                double val1 = Double.parseDouble(val.toString());
                double val2 = Double.parseDouble(key.toString());
                
                // Calculating the temperature difference
                double diff = val2 - val1;
                
                // Incrementing the counter
                c++;
                
                // Emitting key-value pair (Value<c>, diff)
                context.write(new Text("Value" + String.valueOf(c)), new Text(String.valueOf(diff)));
            }
        }
    }

    // Main method
    public static void main(String[] args) throws Exception {
        // Create a Hadoop Configuration
        Configuration conf = new Configuration();

        // Create a new Hadoop job and set its properties
        Job job = Job.getInstance(conf, "Temperature Analysis");
        job.setJarByClass(Task_A_Analysis.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

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

