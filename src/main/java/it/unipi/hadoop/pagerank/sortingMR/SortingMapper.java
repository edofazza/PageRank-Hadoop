package it.unipi.hadoop.pagerank.sortingMR;

import it.unipi.hadoop.pagerank.page.Page;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortingMapper extends Mapper<Text, Text, Text, Page> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
