package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.page.Page;
import it.unipi.hadoop.pagerank.page.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class PageRankMapper extends Mapper<Text, Text, Text, Page> {
    private final Page outputPage = new Page();
    private static Double danglingSum;
    private long nNodes;

    /*@Override
    protected void setup(Context context) {
        this.nNodes = context.getConfiguration().getLong("nNodes", 0);
    }*/

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().trim().split(",");

        Text[] outgoingEdges = Arrays.copyOf(Arrays.copyOfRange(split, 1, split.length), split.length-1, Text[].class);
        outputPage.set(new TextArray(outgoingEdges), Double.parseDouble(split[0]));

        context.write(key, outputPage);

        Text t = new Text("DANGLING");
        if (outgoingEdges.length == 0) // DANDLING
            context.write(t, outputPage);
            //danglingSum += Double.parseDouble(split[0]);
        else {
            for (Text text : outgoingEdges) {
                outputPage.setPagerank(Double.parseDouble(split[0]));
                context.write(text, outputPage);
            }
        }
    }

    /*@Override
    protected void cleanup(Context context) {
        Configuration conf = context.getConfiguration();
        conf.setFloat("danglingsMass", danglingSum.floatValue()/(float) nNodes);
    }*/
}
