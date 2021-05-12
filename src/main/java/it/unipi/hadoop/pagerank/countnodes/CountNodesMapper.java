package it.unipi.hadoop.pagerank.countnodes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This class implements the Mapper that is in charge of generating the pairs (k,1) for each nodes of the document
 * The intermediate key is always k, we need a unique key
 */
public class CountNodesMapper extends Mapper<Object, Text, Text, LongWritable> {
    private final Text outputKey = new Text("n");
    private final LongWritable outputValue = new LongWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();

        if (record == null || record.length() == 0)
            return;

        context.write(outputKey, outputValue);
    }
}
