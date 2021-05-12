package it.unipi.hadoop.pagerank.page;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Page implements WritableComparable<Page> {
    //private Text title;
    private double pagerank;
    private TextArray outgoingEdges;


    //********************************************
    //              CONSTRUCTORS
    //*******************************************
    public Page() {

    }

    public Page(String outEstring) {
        set(stringToTextArray(outEstring));
    }

    public Page(TextArray outElist) {
        set(outElist);
    }

    public Page(TextArray outgoingEdges, double pagerank) {
        set(outgoingEdges);
        this.pagerank = pagerank;
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
    public void set(final TextArray outgoingEdges) {
        this.outgoingEdges = outgoingEdges;
    }

    public static Page copy(final Page page) {
        return new Page(page.getOutgoingEdges(), page.pagerank);
    }

    public TextArray stringToTextArray(String edges) {
        String[] edgeArray = edges.trim().split(",");
        return new TextArray(Arrays.copyOf(edgeArray, edgeArray.length, Text[].class));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(this.pagerank);
        outgoingEdges.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pagerank = dataInput.readDouble();
        outgoingEdges.readFields(dataInput);
    }

    @Override
    public int compareTo(Page o) {
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
        return "Page{" +
                ", outgoingEdges=" + outgoingEdges +     // TODO: FORMAT CORRECTLY
                ", pagerank=" + pagerank +
                '}';
    }
}
