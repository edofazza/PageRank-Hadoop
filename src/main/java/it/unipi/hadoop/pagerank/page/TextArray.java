package it.unipi.hadoop.pagerank.page;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextArray extends ArrayWritable {

    public TextArray() {
        super(TextArray.class);
    }

    public TextArray(Text[] values) {
        super(TextArray.class, values);
    }
}
