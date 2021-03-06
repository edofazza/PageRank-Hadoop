package it.unipi.hadoop.pagerank;

import it.unipi.hadoop.pagerank.dataparserMR.DataParserMapper;
import it.unipi.hadoop.pagerank.dataparserMR.DataParserReducer;
import it.unipi.hadoop.pagerank.dataparserMR.ParserPartitioner;
import it.unipi.hadoop.pagerank.model.Node;
import it.unipi.hadoop.pagerank.model.TextArray;
import it.unipi.hadoop.pagerank.pagerankMR.PageRankMapper;
import it.unipi.hadoop.pagerank.pagerankMR.PageRankReducer;
import it.unipi.hadoop.pagerank.sortingMR.DescendingRankComparator;
import it.unipi.hadoop.pagerank.sortingMR.SortingMapper;
import it.unipi.hadoop.pagerank.sortingMR.SortingReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class PageRank {
    private static final int HOW_MANY_REDUCER = 3; // we take advantage of all the workers available, when it is possible

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: PageRank <input path> <output path> <# of iterations>");
            System.exit(-1);
        }

        if (!dataParserJob(conf, otherArgs[0], "tmp1"))
            System.exit(-1);
        if (!pagerankJob(conf, "tmp1", "tmp2", Integer.parseInt(otherArgs[2]), "tmp1/part-r-00000"))
            System.exit(-1);

        boolean finalStatus = sortingJob(conf, "tmp2/iter" + (Integer.parseInt(otherArgs[2])), otherArgs[1]);
        removeDirectory(conf, "tmp2");

        // TIME
        long end = System.currentTimeMillis();
        end -= start;
        System.out.println("EXECUTION TIME: " + end + " ms");

        if (!finalStatus)
            System.exit(-1);
    }

    private static boolean dataParserJob(Configuration conf, String inPath, String outPath) throws Exception {
        Job job = Job.getInstance(conf, "pageParserJob");
        job.setJarByClass(PageRank.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArray.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(DataParserMapper.class);
        job.setReducerClass(DataParserReducer.class);
        job.setPartitionerClass(ParserPartitioner.class);

        // I can use all the machines for running the reduce task, I will obtain different output files
        job.setNumReduceTasks(HOW_MANY_REDUCER);

        FileInputFormat.addInputPath(job,  new Path(inPath));
        FileOutputFormat.setOutputPath(job,  new Path(outPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    private static boolean pagerankJob(Configuration conf, String inPath, String outPath, int nIter, String inDataPath) throws Exception {
        long nNodes = readNumber(conf, inDataPath, "");
        conf.setLong("nNodes", nNodes);
        boolean result = false;

        for (int i = 0; i < nIter; i++) {
            System.out.println("\n\n\n\n\nITER NUMBER " + (i+1));
            Job job = Job.getInstance(conf, "pageRank");
            job.setJarByClass(PageRank.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Node.class);

            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);

            job.setNumReduceTasks(HOW_MANY_REDUCER);

            // CHECK IF STEP 1
            if (i == 0) {
                FileInputFormat.setInputPaths(job,
                        new Path(inPath + "/part-r-00001"),
                        new Path(inPath + "/part-r-00002"));
                FileOutputFormat.setOutputPath(job, new Path(outPath + "/iter" + (i+1)));
            } else {
                FileInputFormat.setInputPaths(job,
                        new Path(outPath + "/iter" + (i) + "/part-r-00000"),
                        new Path(outPath + "/iter" + (i) + "/part-r-00001"),
                        new Path(outPath + "/iter" + (i) + "/part-r-00002"));
                FileOutputFormat.setOutputPath(job, new Path(outPath + "/iter" + (i+1)));
            }

            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            result = job.waitForCompletion(true);

            if (i == 0)
                removeDirectory(conf, inPath);
            if (i > 0)
                removeDirectory(conf, outPath + "/iter" + i);
        }
        return result;
    }

    private static boolean sortingJob(Configuration conf, String inPath, String outPath) throws Exception {
        Job job  = Job.getInstance(conf, "sorting");
        job.setJarByClass(PageRank.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(SortingMapper.class);
        job.setReducerClass(SortingReducer.class);
        //no. of reduce tasks equal 1 to enforce global sorting
        job.setNumReduceTasks(1);

        job.setSortComparatorClass(DescendingRankComparator.class);

        FileInputFormat.setInputPaths(job,
                new Path(inPath + "/part-r-00000"),
                new Path(inPath + "/part-r-00001"),
                new Path(inPath + "/part-r-00002"));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        return job.waitForCompletion(true);
    }



    //*********************************
    //          UTILITIES
    //*********************************
    private static long readNumber(Configuration conf, String pathString, String pattern)
            throws Exception {
        long result = 0;
        FileSystem hdfs = FileSystem.get(conf);

        BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(new Path(pathString))));
        try {
            String line;
            line=br.readLine();
            while (line != null){
                if (line.startsWith(pattern)) {
                    result = Long.parseLong(line.split("\t")[1]);
                    break;
                }

                // be sure to read the next line otherwise we get an infinite loop
                line = br.readLine();
            }
        } finally {
            // close out the BufferedReader
            br.close();
        }

        return result;
    }

    private static void removeDirectory(Configuration conf, String pathString) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        hdfs.delete(new Path(pathString), true);
    }

}
