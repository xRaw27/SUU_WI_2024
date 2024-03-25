package pl.edu.agh.suu.hadoop.partitioner;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;


public class HourPartitioner <K2, V2> extends Partitioner<Text, Text> implements
        Configurable {
    private Configuration configuration;
    HashMap<String, Integer> hours = new HashMap<String, Integer>();

    /**
     * Set up the hour hash map in the setConf method.
     */
    @Override
    public void setConf(Configuration configuration) {
        /*
         * Add the hours to a HashMap.
         */
        /*
         * TODO implement
         */
    }

    /**
     * Implement the getConf method for the Configurable interface.
     */
    @Override
    public Configuration getConf() {
        return configuration;
    }

    /**
     * You must implement the getPartition method for a partitioner class.
     * This method receives the abbreviation for each hour
     * as its value. (It is the output value from the mapper.)
     * It should return an integer representation of the hour.
     *
     *
     * For this partitioner to work, the job configuration must have been
     * set so that there are exactly 24 reducers.
     */
    public int getPartition(Text key, Text value, int numReduceTasks) {
        /*
         * TODO implement
         * Change the return 0 statement below to return the number of the hour
         * represented by the value parameter.  Use the hours  hashtable to map
         * the string to the hours number: hours.get(value.toString()) and cast it to int.
         */
        return 0;
    }

}





