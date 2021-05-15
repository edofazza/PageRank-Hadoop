package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.model.Node;
import it.unipi.hadoop.pagerank.model.TextArray;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mapper of the Page Rank process
 * KEY_INPUT:       offset of the line read from the file
 * VALUE_INPUT:     one node (its string representation)
 * KEY_OUTPUT:      title of the node
 * VALUE_OUTPUT:    node information or mass to send to the node
 */
public class PageRankMapper extends Mapper<Object, Text, Text, Node> {
    private static final Text outputKey = new Text();
    private static final Node outputNode = new Node();

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
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // FORMAT:
        //      title \t pagerank, outgoing, ...
        String[] valueSplit = value.toString().split("\t");

        // set the title as the key
        outputKey.set(valueSplit[0]);

        // FORMAT:
        //      pagerank, outgoing, ...
        String[] split = valueSplit[1].trim().split(",");

        // TAKE THE LIST OF OUTGOING EDGES
        List<Text> list = new ArrayList<>();
        for (int i = 1; i < split.length; i++) // i = 1 because we skip the pagerank value (see FORMAT)
            list.add(new Text(split[i]));

        Text[] outgoingEdges = list.toArray(new Text[0]);

        outputNode.set(new TextArray(outgoingEdges), Double.parseDouble(split[0]));

        // Send the node information to the reducer (for preserving the graph structure)
        context.write(outputKey, outputNode);

        double massToSend = Double.parseDouble(split[0])/ outgoingEdges.length;
        // Send to each outgoing edge a split of the mass
        for (Text text : outgoingEdges)
        {
            outputNode.set(new TextArray(), massToSend); // We use the Node structure for sending the masses to send
            context.write(text, outputNode);
        }
    }
}
