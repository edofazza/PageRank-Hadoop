package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.model.Node;
import it.unipi.hadoop.pagerank.model.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        List<Text> list= new ArrayList<>();
        //Text[] outgoingEdges = Arrays.copyOf(Arrays.copyOfRange(split, 1, split.length), split.length-1, Text[].class);
        for (int i = 1; i < split.length; i++)
            list.add(new Text(split[i]));

        Text[] outgoingEdges = list.toArray(new Text[0]);

        outputNode.set(new TextArray(outgoingEdges), Double.parseDouble(split[0]));

        // SEND OUTGOING EDGES
        context.write(outputKey, outputNode);

        Text t = new Text("DANGLING");
        if (outgoingEdges.length == 0) // DANGLING
            context.write(t, outputNode);
            //danglingSum += Double.parseDouble(split[0]);
        else {  // IF NOT DANDLING SEND MASS TO OTHER NODES
            double massToSend = Double.parseDouble(split[0])/ outgoingEdges.length;
            for (Text text : outgoingEdges) {
                outputNode.setPagerank(massToSend);
                context.write(text, outputNode);
            }
        }
    }
}
