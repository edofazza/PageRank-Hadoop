package it.unipi.hadoop.pagerank.model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class stores all the status information of a node in the graph
 * It implements WritableComparable because it mut be serializable
 */
public class Node implements WritableComparable<Node> {
    private double pagerank;
    private TextArray outgoingEdges;

    //********************************************
    //              CONSTRUCTORS
    //*******************************************
    public Node() {
        outgoingEdges = null;
    }

    public void set (String representation)
    {
        // FORMAT:
        //      pagerank, outgoing, ...
        String[] split = representation.trim().split(",");

        // TAKE THE LIST OF OUTGOING EDGES
        List<Text> list = new ArrayList<>();
        for (int i = 1; i < split.length; i++) // i = 1 because we skip the pagerank value (see FORMAT)
            list.add(new Text(split[i]));

        Text[] outgoingEdges = list.toArray(new Text[0]);
        this.outgoingEdges = new TextArray(outgoingEdges);
        this.pagerank = Double.parseDouble(split[0]);
    }

    public Node(TextArray outgoingEdges, double pagerank) {
        set(outgoingEdges, pagerank);
    }

    //********************************************
    //              RETRIEVE VALUES
    //*******************************************

    public double getPagerank() {
        return pagerank;
    }

    public TextArray getOutgoingEdges() {
        return outgoingEdges;
    }

    //********************************************
    //              UTILITIES
    //*******************************************
    public void set(final TextArray outgoingEdges, final double pagerank) {
        this.outgoingEdges = outgoingEdges;
        this.pagerank = pagerank;
    }

    public void setPagerank(final double pagerank) {
        this.pagerank = pagerank;
    }

    public static Node copy(final Node node) {
        return new Node(node.getOutgoingEdges(), node.pagerank);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(this.pagerank);
        outgoingEdges.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pagerank = dataInput.readDouble();
        outgoingEdges = new TextArray();
        outgoingEdges.readFields(dataInput);
    }

    @Override
    public int compareTo(Node o) {
        if (this == o)
            return 0;
        else if (this.pagerank > o.pagerank)
            return 1;
        else if (this.pagerank < o.pagerank)
            return -1;
        return 0;
    }

    @Override
    public String toString() {
        return this.pagerank + "," +
                this.outgoingEdges.toString();
    }
}
