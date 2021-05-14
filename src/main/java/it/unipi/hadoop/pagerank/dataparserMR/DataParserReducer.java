package it.unipi.hadoop.pagerank.dataparserMR;

import it.unipi.hadoop.pagerank.model.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer used for parsing the documents and retrieves the interesting information
 * KEY_INPUT: title of the document
 * VALUE_INPUT: array of one element containing the TextArray of the outgoing links of this document
 * KEY_OUTPUT: title of the document
 * VALUE_OUTPUT: a text containing the initial page rank value and the array of the outgoing links of this document, divided by comma
 */
public class DataParserReducer extends Reducer<Text, TextArray, Text, Text> {
    private double initialPageRank; // 1 / N, with N the number of pages (or nodes in the graph)

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
