package it.unipi.hadoop.pagerank.dataparserMR;

import com.google.gson.Gson;
import it.unipi.hadoop.pagerank.model.Node;
import it.unipi.hadoop.pagerank.model.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Reducer used to store the results calculated with the mappers
 * KEY_INPUT:       title of the page   (or special key "")
 * VALUE_INPUT:     array of one element, who contains the array of outgoing links of this page
 *                  (or an array of the partial counters)
 * KEY_OUTPUT:      title of the page   (or special key "")
 * VALUE_OUTPUT:    node information    (or definitive counter value)
 */
public class DataParserReducer extends Reducer<Text, TextArray, Text, Text> {
    private final static double initialPageRank = -1; // an impossible value
    private static Text outputValue = new Text();
    private static TextArray textArray = new TextArray();
    private static Node outputNode = new Node();

    @Override
    protected void reduce(Text key, Iterable<TextArray> values, Context context) throws IOException, InterruptedException {

        if (key.toString().equals("")) // I have received the counting
        {
            // Aggregate the partial results
            long counter = 0;
            for (TextArray value: values)
            {
                counter += Long.parseLong(value.get()[0].toString()); // each time it is in the first element of the array
            }
            outputValue.set(String.valueOf(counter));
        }
        else
        {
            // I will receive only one element in the values list
            textArray = values.iterator().next();
            outputNode.set(textArray, initialPageRank);
            outputValue.set(outputNode.toString());
        }
        context.write(key, outputValue);
    }
}
