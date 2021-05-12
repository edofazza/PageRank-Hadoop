package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.page.Page;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper extends Mapper<Text, Page, Text, Page> {


    @Override
    protected void map(Text key, Page value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
