package it.unipi.hadoop.pagerank.dataparserMR;

import it.unipi.hadoop.pagerank.model.Node;
import it.unipi.hadoop.pagerank.model.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer used to store the results calculated with the mappers and calculate the initial page rank value
 * KEY_INPUT:       title of the page
 * VALUE_INPUT:     array of one element, who contains the array of outgoing links of this page
 * KEY_OUTPUT:      title of the page
 * VALUE_OUTPUT:    node information
 */
public class DataParserReducer extends Reducer<Text, TextArray, Text, Node> {
    private static double initialPageRank; // 1 / N, with N the number of pages (or nodes in the graph)
    // reuse the writable object
    private final Node outputValue = new Node();

    @Override
    protected void setup(Context context) {
        initialPageRank = (double) 1 / Long.parseLong(context.getConfiguration().get("nNodes"));
    }

    @Override
    protected void reduce(Text key, Iterable<TextArray> values, Context context) throws IOException, InterruptedException {
        outputValue.set(values.iterator().next(), initialPageRank);
        context.write(key, outputValue);
    }
}
