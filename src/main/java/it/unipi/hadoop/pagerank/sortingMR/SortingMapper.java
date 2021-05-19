package it.unipi.hadoop.pagerank.sortingMR;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


/**
 * Mapper for the sorting phase
 * KEY_INPUT:       title of the page
 * VALUE_INPUT:     node information
 * KEY_OUTPUT:      page rank of the page
 * VALUE_OUTPUT:    title of the page
 */
public class SortingMapper extends Mapper<Text, Text, DoubleWritable, Text> {
    private static final DoubleWritable outputKey = new DoubleWritable();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String pagerank = value.toString().trim().split(",")[0];
        outputKey.set(Double.parseDouble(pagerank));
        // pagerank as key in order to have automatic sorting
        // title of the page as value
        context.write(outputKey, key);
    }
}
