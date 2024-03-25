package pl.edu.agh.suu.hadoop.billings;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MaxCostMapper extends
        Mapper<Text, Text, Text, DoubleWritable> {

    @Override
    protected void map(Text phoneNumber, Text costAsString, Context context)
            throws IOException, InterruptedException {
        // TODO: collect the maximum cost and the phone number
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        // TODO: emit the maximal found cost and the associated number
    }
}