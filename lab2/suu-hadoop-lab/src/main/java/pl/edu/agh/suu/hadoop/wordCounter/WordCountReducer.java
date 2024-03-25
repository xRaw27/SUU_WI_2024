package pl.edu.agh.suu.hadoop.wordCounter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reduce function gets called once per key grouping, in this case each word.
 * We’ll iterate through the values, which will be numbers, and take a running sum.
 * The final value of this running sum will be the sum of the ones.
 */

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable totalWordCount = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int wordCount = 0;
        Iterator<IntWritable> it=values.iterator();
        while (it.hasNext()) {
            wordCount += it.next().get();
        }
        totalWordCount.set(wordCount);
        context.write(key, totalWordCount);
    }
}
