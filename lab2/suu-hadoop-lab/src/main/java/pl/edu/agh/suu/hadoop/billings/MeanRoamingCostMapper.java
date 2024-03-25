package pl.edu.agh.suu.hadoop.billings;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MeanRoamingCostMapper extends
        Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static final Logger LOGGER = Logger
            .getLogger(MeanRoamingCostMapper.class);
    private final static Text REUSABLE_TEXT = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String [] tokens = value.toString().split(";");
        Text phoneNumber = new Text(tokens[1]);
        if (tokens[6].equals("false")) {
            return;
        }
        DoubleWritable cost = new DoubleWritable(Double.parseDouble(tokens[8]));
        context.write(phoneNumber, cost);
    }
}
