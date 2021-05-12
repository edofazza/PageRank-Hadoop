package it.unipi.hadoop.pagerank.page;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextArray extends ArrayWritable {

    public TextArray(Text[] values) {
        super(Text.class, values);
    }

    public TextArray() {
        super(Text.class);
    }

    @Override
    public Text[] get() {
        Writable[] temp = super.get();
        if (temp != null) {
            int n = temp.length;
            Text[] items = new Text[n];
            for (int i = 0; i < temp.length; i++) {
                items[i] = (Text)temp[i];
            }
            return items;
        } else {
            return null;
        }
    }

    public void set(Text[] values) {
        super.set(values);
    }

    @Override
    public String toString() {
        Text[] values = get();
        StringBuilder finalString = new StringBuilder();
        for (int i=0; i<values.length; i++)
        {
            if (i != 0)
            {
                finalString.append(",").append(values[i]);
            }
            else
            {
                finalString.append(values[i]);
            }
        }
        return finalString.toString();
    }
}
