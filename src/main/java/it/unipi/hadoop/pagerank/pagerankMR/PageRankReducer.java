package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.model.Node;
import it.unipi.hadoop.pagerank.model.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer of the Page Rank process
 * KEY_INPUT:       title of the page
 * VALUE_INPUT:     node information
 * KEY_OUTPUT:      title of the page
 * VALUE_OUTPUT:    node information
 *
 * The new value of page rank is computed in this way:
 *         pagerank' = (1 - damping)/nNodes + damping * (pagerankSum)
 * With pagerankSum the sum of the masses received from the ingoing links
 */
public class PageRankReducer extends Reducer<Text, Node, Text, Node> {
    private static long nNodes;
    private static final Node outputValue = new Node();
    private static final TextArray emptyTextArray = new TextArray();

    private static final double damping = .80;

    @Override
    protected void setup(Context context) {
        nNodes = Long.parseLong(context.getConfiguration().get("nNodes"));
    }

    @Override
    protected void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
        double pagerankSum = 0;
        Node node = null;

        for (Node value: values) {
            // Check if it is a node or not, if it is a node I get the node structure,
            // otherwise I compute the ingoing mass
            if (value.getOutgoingEdges() != null && value.getOutgoingEdges().get().length != 0) // if the array of outgoing edges is not empty means that this is the node structure
                node = Node.copy(value); // deep copy
            else
                pagerankSum += value.getPagerank();
        }

        // If we have received only mass without the Node structure means that this is not a node
        // The mass received is lost
        if (node == null)
            return;

        // If we have received a fake TextArray, we need to restore the structure before emitting the node
        if (node.getOutgoingEdges().get()[0].toString().equals(""))
            node.setOutgoingEdges(emptyTextArray);

        outputValue.set(node.getOutgoingEdges(), (1-damping)/(double) nNodes + damping * pagerankSum);
        context.write(key, outputValue);
    }
}
