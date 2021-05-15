package it.unipi.hadoop.pagerank.countnodes;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This class implements the Mapper that is in charge of generating the pairs (n,1) for each pages of the document
 * KEY_INPUT:       offset of the line read from the file
 * VALUE_INPUT:     one line of the dataset, so one page
 * KEY_OUTPUT:      always 'n', we need a unique key for grouping the value
 * VALUE_OUTPUT:    1
 */
public class CountNodesMapper extends Mapper<Object, Text, Text, LongWritable> {
    private static final Text outputKey = new Text("n");
    private static final LongWritable outputValue = new LongWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();

        if (record == null || record.length() == 0)
            return;

        context.write(outputKey, outputValue);
    }
}
