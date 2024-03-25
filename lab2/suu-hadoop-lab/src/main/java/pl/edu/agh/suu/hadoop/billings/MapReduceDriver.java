package pl.edu.agh.suu.hadoop.billings;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class MapReduceDriver extends Configured implements Tool {
    private static final Logger LOGGER = Logger.getLogger(MapReduceDriver.class);

    public static void main(String[] args) throws Exception {
        System.out.println("MapReduceDriver, number of arguments: " + args.length);
        System.out.print ("ARGS= ");
        for (String arg: args)
            System.out.print (arg + " ");
        System.out.println();

        int exitCode = ToolRunner.run(new MapReduceDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Maximum Roaming Call Cost");
        job.setJobName("<your Name> map reduce name");
        job.setJarByClass(getClass());

        // configure output and input source
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // configure mapper and reducer
        job.setMapperClass(MeanRoamingCostMapper.class);
        job.setCombinerClass(MeanRoamingCostReducer.class);
        job.setReducerClass(MeanRoamingCostReducer.class);

        // configure output
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Removes the given directory from HDFS.
     *
     * @param intermediatePath
     *            a path of the directory to remove
     * @throws IOException
     */
    private void deleteIntermediateDir(Path intermediatePath)
            throws IOException {
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(intermediatePath))
            fs.delete(intermediatePath, true);
    }
}

/***********************************
 public int run(String[] args) throws Exception {
 String tempDir = "/" + getClass().getSimpleName() + "-temp";
 deleteIntermediateDir(new Path(tempDir));

 try {
 JobControl control = new JobControl("Combined Workflow-Example");
 ControlledJob step1 = new ControlledJob(getMeanRoamingCostJob(
 args[0], tempDir), null);
 ControlledJob step2 = new ControlledJob(getMaxCostJob(tempDir,
 args[1]), Arrays.asList(step1));


 control.addJob(step1);
 control.addJob(step2);

 Thread workflowThread = new Thread(control, "Workflow-Thread");
 workflowThread.setDaemon(true);
 workflowThread.start();

 while (!control.allFinished())
 Thread.sleep(500);

 if (control.getFailedJobList().size() > 0) {
 LOGGER.error(control.getFailedJobList().size()
 + " jobs failed!");
 for (ControlledJob job : control.getFailedJobList()) {
 LOGGER.error(job.getJobName() + " failed");
 }
 } else
 LOGGER.info("Success!! Workflow completed ["
 + control.getSuccessfulJobList().size() + "] jobs");

 return control.getFailedJobList().size() > 0 ? 1 : 0;
 } finally {
 deleteIntermediateDir(new Path(tempDir));
 }
 }

 private Job getMeanRoamingCostJob(String inputDir, String tempDir)
 throws IOException {
 Job job = Job.getInstance(getConf(), "Maximum Roaming Call Cost");
 job.setJobName("User Mean Roaming Call Cost");
 job.setJarByClass(getClass());

 // configure output and input source
 TextInputFormat.addInputPath(job, new Path(inputDir));
 job.setInputFormatClass(TextInputFormat.class);

 // configure mapper and reducer
 job.setMapperClass(MeanRoamingCostMapper.class);
 job.setCombinerClass(MeanRoamingCostReducer.class);
 job.setReducerClass(MeanRoamingCostReducer.class);

 // configure output
 TextOutputFormat.setOutputPath(job, new Path(tempDir));
 job.setOutputFormatClass(TextOutputFormat.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(DoubleWritable.class);

 return job;
 }

 private Job getMaxCostJob(String tempDir, String outputDir)
 throws IOException {
 Job job = Job.getInstance(getConf(), "Maximum Cost");
 job.setJobName("User Maximum Cost");
 job.setJarByClass(getClass());

 // configure output and input source
 KeyValueTextInputFormat.addInputPath(job, new Path(tempDir));
 job.setInputFormatClass(KeyValueTextInputFormat.class);

 // configure mapper and reducer
 job.setMapperClass(MaxCostMapper.class);
 job.setCombinerClass(MaxCostReducer.class);
 job.setReducerClass(MaxCostReducer.class);

 // configure output
 TextOutputFormat.setOutputPath(job, new Path(outputDir));
 job.setOutputFormatClass(TextOutputFormat.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(DoubleWritable.class);

 return job;
 }



 ************/