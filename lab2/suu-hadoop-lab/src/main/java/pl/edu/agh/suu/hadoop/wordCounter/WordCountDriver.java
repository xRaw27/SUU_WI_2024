package pl.edu.agh.suu.hadoop.wordCounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * The purpose of the driver is to orchestrate the jobs. The first few lines of main are all
 * about parsing command line arguments. Then we start setting up the job object by
 * telling it what classes to use for computations and what input paths and output paths to
 * use. It’s just important to make sure the class names match up with the
 * classes you wrote and that the output key and value types match up with the output
 * types of the mapper.
 *
 * One way you’ll see this code change from pattern to pattern is the usage of job.setCom
 * binerClass. In some cases, the combiner simply cannot be used due to the nature of
 * the reducer. In other cases, the combiner class will be different from the reducer class.
 * The combiner is very effective in the Word Count program and is quite simple to
 * activate.
 *
 * As in the mapper, we specify the input and output types via the template parent class.
 * Also like the mapper, the types correspond to the same things: input key, input value,
 * output key, and output value. The input key and input value data types must match the
 * output key/value types from the mapper. The output key and output value data types
 * must match the types that the job’s configured FileOutputFormat is expecting. In this
 * case, we are using the default TextOutputFormat, which can take any two Writable
 * objects as output.
 *
 */


public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        System.out.println("number of arguments: " + args.length);
        System.out.print ("ARGS= ");
        for (String arg: args)
            System.out.print (arg + " ");
        System.out.println();

        if (args.length != 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration(), "Example SUU word count");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(WordCountDriver.class);

        job.submit();
    }
}