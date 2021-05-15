package it.unipi.hadoop.pagerank.sortingMR;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Comparator used for ordering in descending order the intermediate keys in the Sorting Phase
 */
public class DescendingRankComparator extends WritableComparator {
    protected DescendingRankComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable key1 = (DoubleWritable) w1;
        DoubleWritable key2 = (DoubleWritable) w2;
        return -1 * key1.compareTo(key2); // the opposite of the default behavior
    }
}

