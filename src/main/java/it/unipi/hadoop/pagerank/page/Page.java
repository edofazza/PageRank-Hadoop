package it.unipi.hadoop.pagerank.page;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Page implements WritableComparable<Page> {
    private Text title;
    private double pagerank;
    private TextArray outgoingEdges;


    //********************************************
    //              CONSTRUCTORS
    //*******************************************
    public Page() {

    }

    public Page(String title, String outEstring) {
        set(new Text(title), stringToTextArray(outEstring));
    }

    public Page(String title, TextArray outElist) {
        set(new Text(title), outElist);
    }

    public Page(Text title, String outEstring) {
        set(title, stringToTextArray(outEstring));
    }

    public Page(Text title, TextArray outElist) {
        set(title, outElist);
    }

    public Page(Text title, TextArray outgoingEdges, double pagerank) {
        set(title, outgoingEdges);
        this.pagerank = pagerank;
    }

    //********************************************
    //              RETRIEVE VALUES
    //*******************************************

    public Text getTitle() {
        return title;
    }

    public double getPagerank() {
        return pagerank;
    }

    public TextArray getOutgoingEdges() {
        return outgoingEdges;
    }

    //********************************************
    //              UTILITIES
    //*******************************************
    public void set(final Text title, final TextArray outgoingEdges) {
        this.title = title;
        this.outgoingEdges = outgoingEdges;
    }

    public static Page copy(final Page page) {
        return new Page(page.getTitle(), page.getOutgoingEdges(), page.pagerank);
    }

    public TextArray stringToTextArray(String edges) {
        String[] edgeArray = edges.trim().split(",");
        return new TextArray(Arrays.copyOf(edgeArray, edgeArray.length, Text[].class));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        title.write(dataOutput);
        dataOutput.writeDouble(this.pagerank);
        outgoingEdges.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        title.readFields(dataInput);
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
                "title=" + title.toString() +
                ", outgoingEdges=" + outgoingEdges +     // TODO: FORMAT CORRECTLY
                ", pagerank=" + pagerank +
                '}';
    }
}
