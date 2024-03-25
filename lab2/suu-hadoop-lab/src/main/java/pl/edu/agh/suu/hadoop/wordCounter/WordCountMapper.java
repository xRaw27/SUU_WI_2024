package pl.edu.agh.suu.hadoop.wordCounter;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The first major thing to notice is the type of the parent class:
 * Mapper<Object, Text, Text, IntWritable>
 * They map to the types of the input key, input value, output key, and output value, respectively.
 * We don’t care about the key of the input in this case, so that’s why we use
 * Object. The data coming in is Text (Hadoop’s special String type) because we are
 * reading the data as a line-by-line text document. Our output key and value are Text and
 * IntWritable because we will be using the word as the key and the count as the value.
 *
 * The mapper input key and value data types are dictated by the job’s
 * configured FileInputFormat. The default implementation is the TextInputFormat,
 * which provides the number of bytes read so far in the
 * file as the key in a LongWritable object and the line of text as the value
 * in a Text object. These key/value data types are likely to change if you
 * are using different input formats.
 *
 * Finally, for each token (i.e., word) we emit the word with the number 1, which means
 * we saw the word once. The framework then takes over to shuffle and sorts the key/value
 * pairs to reduce tasks.
 *
 */


public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value,
                    Context contex) throws IOException, InterruptedException {
        // Break line into words for processing
        StringTokenizer wordList = new StringTokenizer(value.toString());
        while (wordList.hasMoreTokens()) {
            word.set(wordList.nextToken());
            contex.write(word, one);
        }
    }
}