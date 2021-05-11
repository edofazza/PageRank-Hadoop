package it.unipi.hadoop.pagerank.countnodes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountNodesMapper extends Mapper<Object, Text, Text, LongWritable> {
    private final Text outputKey = new Text("k");
    private final LongWritable outputValue = new LongWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();

        if (record == null || record.length() == 0)
            return;

        context.write(outputKey, outputValue);
    }
}
