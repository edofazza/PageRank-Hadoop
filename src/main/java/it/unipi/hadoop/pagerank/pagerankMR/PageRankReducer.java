package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.page.Page;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, Page, Text, Text> {
    private long nNodes;
    private double danglingsMass;
    private static Double danglingsSum;

    private final double damping = .8;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.nNodes = context.getConfiguration().getLong("nNodes", 10);
    }

    /*
       pagerank' = (1 - damping)/nNodes + damping/nNodes * (danglingMass + pagerank)
     */
    @Override
    protected void reduce(Text key, Iterable<Page> values, Context context) throws IOException, InterruptedException {
        double pagerankSum = 0;
        Page page = null;

        Text outputValue = new Text();

        /*
            Check if it is a node or not, if it is a node I get the outgoings links,
            otherwise I compute the ingoing mass
         */
        for (Page value: values) {
            if (value.getOutgoingEdges().get().length != 0 && value.getOutgoingEdges() != null)
                page = Page.copy(value);
            else
                pagerankSum += value.getPagerank();
        }

        if (page == null)
            return;

        if (page.getOutgoingEdges().get().length == 0) {// DANGLING
            outputValue.set(Double.toString(pagerankSum));
            context.write(key, outputValue);
        } else {
            page.setPagerank(
                    //(1-damping)/(double) nNodes + damping * (danglingsMass + pagerankSum)
                    (1-damping)/(double) nNodes + damping * pagerankSum
            );
            outputValue.set(page.toString());
            context.write(key, outputValue);
        }
    }
}
