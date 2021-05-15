package it.unipi.hadoop.pagerank.dataparserMR;

import it.unipi.hadoop.pagerank.model.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer used to store the results calculated with the mappers and calculate the initial page rank value
 * KEY_INPUT:       title of the page
 * VALUE_INPUT:     array of one element containing the array of outgoing links of this page
 * KEY_OUTPUT:      title of the page
 * VALUE_OUTPUT:    text representation of the node information
 */
public class DataParserReducer extends Reducer<Text, TextArray, Text, Text> {
    private static double initialPageRank; // 1 / N, with N the number of pages (or nodes in the graph)

    // reuse the writable objects
    private final Text outputValue = new Text();

    @Override
    protected void setup(Context context) {
        initialPageRank = (double) 1 / Long.parseLong(context.getConfiguration().get("nNodes"));
    }

    @Override
    protected void reduce(Text key, Iterable<TextArray> values, Context context) throws IOException, InterruptedException {
        // I will obtain only one array for each title (key)
        String value = initialPageRank + "," + values.iterator().next().toString();
        outputValue.set(value);
        context.write(key, outputValue);
    }
}
