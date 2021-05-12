package it.unipi.hadoop.pagerank.dataparserMR;

import it.unipi.hadoop.pagerank.page.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataParserReducer extends Reducer<Text, TextArray, Text, Text> {
    private long nPages;
    private double initialPageRank;

    // reuse the writable objects
    private final Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        nPages = Integer.parseInt(context.getConfiguration().get("nNodes"));
        //nPages = Job.getInstance(context.getConfiguration()).getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        initialPageRank = (double) 1 / nPages;
    }

    @Override
    protected void reduce(Text key, Iterable<TextArray> values, Context context) throws IOException, InterruptedException {
        // I will obtain only one array for each title (key)
        String value = initialPageRank + "," + values.iterator().next().toString();
        outputValue.set(value);
        context.write(key, outputValue);
    }
}
