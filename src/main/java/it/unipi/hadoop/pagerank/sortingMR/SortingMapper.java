package it.unipi.hadoop.pagerank.sortingMR;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortingMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    private DoubleWritable outputKey = new DoubleWritable();
    private Text outputValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] keyValueSplit = value.toString().split("\t");
        outputValue.set(keyValueSplit[0]);
        String pagerank = keyValueSplit[1].trim().split(",")[0];
        outputKey.set(Double.parseDouble(pagerank));

        // pagerank as key in order to have automatic sorting
        // title of the page as value
        context.write(outputKey, outputValue);
    }
}
