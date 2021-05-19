package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.model.Node;
import it.unipi.hadoop.pagerank.model.TextArray;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Mapper of the Page Rank process
 * KEY_INPUT:       title of the page
 * VALUE_INPUT:     node information
 * KEY_OUTPUT:      title of the page
 * VALUE_OUTPUT:    node information or mass to send to the node
 */
public class PageRankMapper extends Mapper<Text, Text, Text, Node> {
    private static final Node outputNode = new Node();
    private static long nNodes;
    private static final TextArray emptyTextArray = new TextArray(); // empty text array

     /*
        SCHEMA:
            (1) -> (2) (3)
            (2) -> ...
            OUTPUT:
                (1) -> (2) (3)
                (2) mass(1)
                (3) mass(1)

                (2) -> ...
                ... -> mass(2)

            REDUCER
                (2) -> ... mass
         */

    @Override
    protected void setup(Context context) {
        nNodes = Long.parseLong(context.getConfiguration().get("nNodes"));
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        outputNode.set(value.toString());

        if (outputNode.getPagerank() == -1) // if it is the first time for this record
            outputNode.setPagerank((double)1/nNodes);

        // Send the node information to the reducer (for preserving the graph structure)
        context.write(key, outputNode);

        double massToSend = outputNode.getPagerank() / outputNode.getOutgoingEdges().get().length;

        // Send to each outgoing edge a split of the mass
        for (Text title : outputNode.getOutgoingEdges().get())
        {
            outputNode.set(emptyTextArray, massToSend); // We use the Node structure for sending the masses
            context.write(title, outputNode);
        }
    }
}
