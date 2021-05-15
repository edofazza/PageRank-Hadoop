package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.model.Node;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, Node, Text, Text> {
    private static long nNodes;
    private static final Text outputValue = new Text();

    private final double damping = .8;

    @Override
    protected void setup(Context context) {
        this.nNodes = Long.parseLong(context.getConfiguration().get("nNodes"));
    }

    /*
       pagerank' = (1 - damping)/nNodes + damping/nNodes * (danglingMass + pagerankSum)
     */
    @Override
    protected void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
        double pagerankSum = 0;
        Node node = null;

        /*
            Check if it is a node or not, if it is a node I get the outgoings links,
            otherwise I compute the ingoing mass
         */
        for (Node value: values) {
            if (value.getOutgoingEdges().get().length != 0 && value.getOutgoingEdges() != null)
                node = Node.copy(value);
            else
                pagerankSum += value.getPagerank();
        }

        if (node == null)
            return;

        if (node.getOutgoingEdges().get().length == 0) {// DANGLING
            outputValue.set(Double.toString(pagerankSum));
            context.write(key, outputValue);
        } else {
            node.setPagerank(
                    //(1-damping)/(double) nNodes + damping * (danglingsMass + pagerankSum)
                    (1-damping)/(double) nNodes + damping * pagerankSum
            );
            outputValue.set(node.toString());
            context.write(key, outputValue);
        }
    }
}
