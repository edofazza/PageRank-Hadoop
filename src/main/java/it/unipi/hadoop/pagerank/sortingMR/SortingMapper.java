package it.unipi.hadoop.pagerank.sortingMR;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortingMapper extends Mapper<Text, Text, Text, Text> {
    private Text outputKey = new Text();;

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String pagerank = value.toString().trim().split(",")[0];
        outputKey.set(pagerank);

        // pagerank as key in order to have automatic sorting
        // title of the page as value
        context.write(outputKey, key);
    }
}
