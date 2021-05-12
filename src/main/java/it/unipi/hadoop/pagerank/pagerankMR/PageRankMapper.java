package it.unipi.hadoop.pagerank.pagerankMR;

import it.unipi.hadoop.pagerank.page.Page;
import it.unipi.hadoop.pagerank.page.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper extends Mapper<Text, Page, Text, Page> {
    private final Text dangling = new Text("d");
    private final Page outputPage = new Page();

    @Override
    protected void map(Text key, Page value, Context context) throws IOException, InterruptedException {
        // check if the node is a dangling node
        if (value.getOutgoingEdges().get().length == 0) {
            context.write(dangling, value);
            return;
        }
        context.write(key, outputPage);

        Text[] texts = (Text[]) value.getOutgoingEdges().get();
        double p = value.getPagerank() / value.getOutgoingEdges().get().length;

        for (Text text: texts) {
            outputPage.setPagerank(p);
            context.write(text, outputPage);
        }
    }
}
