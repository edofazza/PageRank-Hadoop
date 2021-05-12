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
    private final Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        nPages = Integer.parseInt(context.getConfiguration().get("nNodes"));
        initialPageRank = (double) 1 / nPages;
    }

    @Override
    protected void reduce(Text key, Iterable<TextArray> values, Context context) throws IOException, InterruptedException {
        // I will obtain only one TextArray for each title (key)
        StringBuilder value = new StringBuilder(initialPageRank + "\t");
        if (values.iterator().hasNext())
        {
            TextArray textArray = values.iterator().next();
            System.out.println(textArray);
        }
        /*int i=0;
        for (Text text : values.iterator().next().get())
        {
            if (i != 0)
            {
                value.append(text.toString());
            }
            else
            {
                value.append(", ").append(text.toString());
            }
            i++;
        }*/
        outputValue.set(value.toString());
        context.write(key, outputValue);
        // Title, pageRank, outgoingLinks
    }
}
