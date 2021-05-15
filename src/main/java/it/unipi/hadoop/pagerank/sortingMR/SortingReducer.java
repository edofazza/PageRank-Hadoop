package it.unipi.hadoop.pagerank.sortingMR;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer for the sorting phase
 * KEY_INPUT:       Page rank value
 * VALUE_INPUT:     Titles of all the pages that have this value of pagerank
 * KEY_OUTPUT:      Title of the page
 * VALUE_OUTPUT:    Page rank value of the page
 */
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
