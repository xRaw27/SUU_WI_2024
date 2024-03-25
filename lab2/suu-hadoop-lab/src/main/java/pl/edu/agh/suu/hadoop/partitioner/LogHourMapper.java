package pl.edu.agh.suu.hadoop.partitioner;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LogHourMapper extends Mapper<LongWritable, Text, Text, Text>{
    /**
     * Example input line:
     * 2001-07-01T02:00:03;+421605555550;+48710901405;00:42:12;;Koszalin;true;Telefonica;7.84;CELL;Telefonica-50;AMR
     *
     */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        /* TODO: implement */

    }
}
