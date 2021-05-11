package it.unipi.hadoop.pagerank.countnodes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class CountNodesReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        LongWritable outputValye = new LongWritable(values.spliterator().getExactSizeIfKnown());
        context.write(key, outputValye);
    }
}
