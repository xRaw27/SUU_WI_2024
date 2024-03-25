package pl.edu.agh.suu.hadoop.partitioner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class ProcessLogsDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: ProcessLogs <input dir> <output dir>\n");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(ProcessLogsDriver.class);
        job.setJobName("Process Billing Logs");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(LogHourMapper.class);
        job.setReducerClass(NumberReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*
         * Set up the partitioner. Specify 24 reducers - one for each
         * hour of the day. The partitioner class must have a
         * getPartition method that returns a number between 0 and 23.
         * This number will be used to assign the intermediate output
         * to one of the reducers.
		 *
         * Specify the partitioner class.
         */

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
