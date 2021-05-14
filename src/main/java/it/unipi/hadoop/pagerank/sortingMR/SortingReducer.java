package it.unipi.hadoop.pagerank.sortingMR;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortingReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    private Text outputValue = new Text();

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value: values) {
            outputValue.set(Double.toString(key.get()));
            context.write(value, outputValue);
        }
    }
}
