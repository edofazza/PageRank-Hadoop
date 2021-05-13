package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.page.Page;
import it.unipi.hadoop.pagerank.page.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PageRankMapper extends Mapper<Object, Text, Text, Page> {
    private final Text outputKey = new Text();
    private final Page outputPage = new Page();
    private static Double danglingSum;
    private long nNodes;

    /*@Override
    protected void setup(Context context) {
        this.nNodes = context.getConfiguration().getLong("nNodes", 0);
    }*/

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] keyValueSplit = value.toString().split("\t");
        outputKey.set(keyValueSplit[0]);

        String[] split = keyValueSplit[1].trim().split(",");

        List<Text> list= new ArrayList<>();
        //Text[] outgoingEdges = Arrays.copyOf(Arrays.copyOfRange(split, 1, split.length), split.length-1, Text[].class);
        for (int i = 1; i < split.length; i++)
            list.add(new Text(split[i]));

        Text[] outgoingEdges = list.toArray(new Text[0]);

        outputPage.set(new TextArray(outgoingEdges), Double.parseDouble(split[0]));

        context.write(outputKey, outputPage);

        Text t = new Text("DANGLING");
        if (outgoingEdges.length == 0) // DANDLING
            context.write(t, outputPage);
            //danglingSum += Double.parseDouble(split[0]);
        else {  // IF NOT DANDLING SEND MASS TO OTHER NODES
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
