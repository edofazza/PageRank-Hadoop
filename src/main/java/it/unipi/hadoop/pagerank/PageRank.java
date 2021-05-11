package it.unipi.hadoop.pagerank;

import it.unipi.hadoop.pagerank.countnodes.CountNodesMapper;
import it.unipi.hadoop.pagerank.countnodes.CountNodesReducer;
import it.unipi.hadoop.pagerank.dataparserMR.DataParserMapper;
import it.unipi.hadoop.pagerank.dataparserMR.DataParserReducer;
import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: PageRank <input path> <output path> <# of iterations>");
            System.exit(-1);
        }

        countNodesJob(conf, otherArgs[0], otherArgs[1]);
        dataParserJob(conf, otherArgs[0], otherArgs[1]);
        //pagerankJob(conf, otherArgs[1], Integer.parseInt(otherArgs[2]));
        //sortingJob(conf);
    }

    private static void countNodesJob (Configuration conf, String inPath, String outPath) throws Exception {
        Job job  = Job.getInstance(conf, " pageRankSorter");
        job.setJarByClass(PageRank.class);

        job.setOutputKeyClass( Text.class);
        job.setOutputValueClass( Text .class);
        job.setMapperClass(CountNodesMapper.class);
        job.setReducerClass(CountNodesReducer.class);
        //no. of reduce tasks equal 1 to enforce global sorting
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job,  new Path(inPath));
        FileOutputFormat.setOutputPath(job,  new Path(outPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //job.setSortComparatorClass((Class<? extends RawComparator>) ReverseComparator.class);

        job.waitForCompletion(true);
    }

    private static void dataParserJob(Configuration conf, String inPath, String outPath) throws Exception {
        Job job  = Job.getInstance(conf, " pageRankSorter");
        job.setJarByClass(PageRank.class);

        job.setOutputKeyClass( Text.class);
        job.setOutputValueClass( Text .class);
        job.setMapperClass(DataParserMapper.class);
        job.setReducerClass(DataParserReducer.class);
        //no. of reduce tasks equal 1 to enforce global sorting
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job,  new Path(inPath));
        FileOutputFormat.setOutputPath(job,  new Path(outPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //job.setSortComparatorClass((Class<? extends RawComparator>) ReverseComparator.class);

        job.waitForCompletion(true);
    }

    private static void pagerankJob(Configuration conf, String inPath, int nIter) {

    }

    private static void sortingJob(Configuration conf) {

    }
}
