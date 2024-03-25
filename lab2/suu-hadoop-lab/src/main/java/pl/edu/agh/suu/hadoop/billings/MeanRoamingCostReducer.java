package pl.edu.agh.suu.hadoop.billings;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class MeanRoamingCostReducer extends
        Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text phoneNumber, Iterable<DoubleWritable> costs,
                          Context context) throws IOException, InterruptedException {

        int counter = 0;
        double costSum = 0;
        for (DoubleWritable cost : costs) {
            counter += 1;
            costSum += cost.get();
        }
        DoubleWritable avgCost = new DoubleWritable(costSum / counter);
        context.write(phoneNumber, avgCost);
    }
}