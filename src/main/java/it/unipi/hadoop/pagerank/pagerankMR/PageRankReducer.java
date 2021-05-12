package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.page.Page;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, Page, Text, Page> {


    @Override
    protected void reduce(Text key, Iterable<Page> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
