package pl.edu.agh.suu.hadoop.billings;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MaxCostReducer extends
        Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text phoneNumber, Iterable<DoubleWritable> maxCosts,
                          Context context) throws IOException, InterruptedException {
        // TODO: check whether the list of values has only one element
        // TODO: if the new max cost is greater them the one which has been
        // found so far, store it
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // TODO: emit the found maximum cost with the source number
    }
}