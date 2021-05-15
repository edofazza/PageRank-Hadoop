package it.unipi.hadoop.pagerank.countnodes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * This class implements the Reducer that is in charge of counting the number of nodes
 * The reducer will obtains one (key, list of values) pair, so there will be only one execution of the reduce function
 * The Key will be 'n', the sum of the list of values will give the number of nodes
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
