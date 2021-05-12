package it.unipi.hadoop.pagerank.dataparserMR;

import it.unipi.hadoop.pagerank.page.Page;
import it.unipi.hadoop.pagerank.page.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class DataParserReducer extends Reducer<Text, TextArray, Text, Text> {
    private int nPages;
    private double initialPageRank;
    //private final Page page = new Page();
    private final Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        nPages = Integer.parseInt(context.getConfiguration().get("nPages"));
        initialPageRank = (double) 1 / nPages;
    }

    @Override
    protected void reduce(Text key, Iterable<TextArray> values, Context context) throws IOException, InterruptedException {
        // I will obtain only one TextArray for each title (key)
        String value = initialPageRank + "\t" + Arrays.toString(values.iterator().next().get());
        outputValue.set(value);
        //page.set(values.iterator().next(), initialPageRank);
        context.write(key, outputValue);
        // Title, pageRank, outgoingLinks
    }
}
