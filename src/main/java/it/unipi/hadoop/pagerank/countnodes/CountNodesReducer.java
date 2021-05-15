package it.unipi.hadoop.pagerank.countnodes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * This class implements the Reducer that is in charge of counting the number of pages
 * The reducer will obtains one (key, list of values) pair, so there will be only one execution of the reduce function
 * KEY_INPUT:       always 'n'
 * VALUE_INPUT:     list of the values
 * KEY_OUTPUT:      always 'n'
 * VALUE_OUTPUT:    sum of the values of the list, so the number of pages
 */
public class CountNodesReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private static final LongWritable outputValue = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0;
        for (LongWritable value: values) {
            sum += value.get();
        }
        outputValue.set(sum);
        context.write(key, outputValue);
    }
}
