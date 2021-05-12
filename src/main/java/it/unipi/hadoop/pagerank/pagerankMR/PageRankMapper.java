package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.page.Page;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper extends Mapper<Text, Page, Text, Page> {
    private final Page outputPage = new Page();
    private static Double danglingSum;
    private long nNodes;

    @Override
    protected void setup(Context context) {
        this.nNodes = context.getConfiguration().getLong("nNodes", 0);
    }

    @Override
    protected void map(Text key, Page value, Context context) throws IOException, InterruptedException {
        if (value.getOutgoingEdges().get().length == 0)
            danglingSum += value.getPagerank();

        context.write(key, outputPage);

        Text[] texts = (Text[]) value.getOutgoingEdges().get();
        double p = value.getPagerank() / value.getOutgoingEdges().get().length;

        for (Text text: texts) {
            outputPage.setPagerank(p);
            context.write(text, outputPage);
        }
    }

    @Override
    protected void cleanup(Context context) {
        Configuration conf = context.getConfiguration();
        conf.setFloat("danglingsMass", danglingSum.floatValue()/(float) nNodes);
    }
}
