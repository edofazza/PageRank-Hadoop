package it.unipi.hadoop.pagerank.sortingMR;

import it.unipi.hadoop.pagerank.page.Page;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortingReducer extends Reducer<Text, Page, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Page> values, Context context) throws IOException, InterruptedException {

    }
}
