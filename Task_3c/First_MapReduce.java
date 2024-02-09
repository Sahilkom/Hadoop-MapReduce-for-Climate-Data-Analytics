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

public class First_MapReduce {

    // Mapper Class
    public static class First_TemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        // Map method processes each input record and emits key-value pairs
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Splitting the input line into tokens using ","
            String[] tokens = value.toString().split(",");
            
            // Extracting relevant information from the input
            String station = tokens[0];
            String date = tokens[1];
            String tempType = tokens[2];
            String tempValue = tokens[3];
            
            // Filtering for TMAX or TMIN types
            if (tempType.equals("TMAX") || tempType.equals("TMIN")) {
                context.write(new Text(date + " " + station), new Text(tempType + "," + tempValue));
            }
        }
    }

    // Reducer Class
    public static class First_TemperatureReducer extends Reducer<Text, Text, Text, Text> {

        // Reduce method processes the intermediate key-value pairs and emits final results
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String maxVal = null;
            String minVal = null;
            
            // Iterating through values for each date and station
            for (Text val : values) {
                // Splitting the value into temperature type and value
                String[] tokens = val.toString().split(",");
                String tempType = tokens[0];
                String tempValue = tokens[1];
                
                // Storing TMAX and TMIN values
                if (tempType.equals("TMAX")) {
                    maxVal = tempValue;
                } else if (tempType.equals("TMIN")) {
                    minVal = tempValue;
                }
            }
            
            // Calculating temperature difference and emitting the result
            if (maxVal != null && minVal != null) {
                double maxi = Double.parseDouble(maxVal);
                double mini = Double.parseDouble(minVal);
                
                // Adjusting values if they are outside a certain range
                if (maxi > 10.0 || maxi < -10.0) {
                    maxi = maxi / 10.0;
                }
                if (mini > 10.0 || mini < -10.0) {
                    mini = mini / 10.0;
                }

                double diff = maxi - mini;
                context.write(key, new Text(String.valueOf(diff)));
            } else {
                context.write(key, new Text("0"));
            }
        }
    }

    // Main method
    public static void main(String[] args) throws Exception {
        // Create a Hadoop Configuration
        Configuration conf = new Configuration();

        // Create a new Hadoop job and set its properties
        Job job = Job.getInstance(conf, "First class Analysis");
        job.setJarByClass(First_MapReduce.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(First_TemperatureMapper.class);
        job.setReducerClass(First_TemperatureReducer.class);

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

