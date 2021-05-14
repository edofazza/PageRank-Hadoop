package it.unipi.hadoop.pagerank;

import it.unipi.hadoop.pagerank.countnodes.CountNodesMapper;
import it.unipi.hadoop.pagerank.countnodes.CountNodesReducer;
import it.unipi.hadoop.pagerank.dataparserMR.DataParserMapper;
import it.unipi.hadoop.pagerank.dataparserMR.DataParserReducer;
import it.unipi.hadoop.pagerank.page.Page;
import it.unipi.hadoop.pagerank.page.TextArray;
import it.unipi.hadoop.pagerank.pagerankMR.PageRankMapper;
import it.unipi.hadoop.pagerank.pagerankMR.PageRankReducer;
import it.unipi.hadoop.pagerank.sortingMR.SortingMapper;
import it.unipi.hadoop.pagerank.sortingMR.SortingReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class PageRank {
    private static final int HOW_MANY_REDUCER = 3;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        long nNodes = 0;

        if (otherArgs.length != 3) {
            System.err.println("Usage: PageRank <input path> <output path> <# of iterations>");
            System.exit(-1);
        }

        if (!countNodesJob(conf, otherArgs[0], "tmp0"))
            System.exit(-1);
        if (!dataParserJob(conf, otherArgs[0], "tmp1", "tmp0/part-r-00000"))
            System.exit(-1);
        if (!pagerankJob(conf, "tmp1", "tmp2", Integer.parseInt(otherArgs[2])))
            System.exit(-1);
        System.exit(sortingJob(conf, "tmp2", "tmp3") ? 0 : 1);
    }

    private static boolean countNodesJob (Configuration conf, String inPath, String outPath) throws Exception {
        Job job  = Job.getInstance(conf, " countNodes");
        job.setJarByClass(PageRank.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(CountNodesMapper.class);
        job.setCombinerClass(CountNodesReducer.class);
        job.setReducerClass(CountNodesReducer.class);
        //no. of reduce tasks equal 1 to enforce global sorting
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    private static boolean dataParserJob(Configuration conf, String inPath, String outPath, String inDataPath) throws Exception {
        conf.setLong("nNodes", readNumber(conf, inDataPath, "n"));

        Job job = Job.getInstance(conf, "pageParserJob");
        job.setJarByClass(PageRank.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextArray.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(DataParserMapper.class);
        job.setReducerClass(DataParserReducer.class);

        // I can use all the machines for running the reduce task, i will obtain 3 different output files
        job.setNumReduceTasks(HOW_MANY_REDUCER);

        FileInputFormat.addInputPath(job,  new Path(inPath));
        FileOutputFormat.setOutputPath(job,  new Path(outPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    private static boolean pagerankJob(Configuration conf, String inPath, String outPath, int nIter) throws Exception {
        Job job  = Job.getInstance(conf, "pageRank");
        job.setJarByClass(PageRank.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Page.class);

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        //no. of reduce tasks equal 1 to enforce global sorting
        job.setNumReduceTasks(1);

        /*MultipleInputs.addInputPath(job, new Path(inPath + "/part-r-00000"), FileInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(inPath + "/part-r-00001"), FileInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(inPath + "/part-r-00002"), FileInputFormat.class);*/
        // TODO: ADD INPUT
        FileInputFormat.addInputPath(job,  new Path(inPath+ "/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        return job.waitForCompletion(true);
    }

    private static boolean sortingJob(Configuration conf, String inPath, String outPath) throws Exception {
        Job job  = Job.getInstance(conf, "sorting");
        job.setJarByClass(PageRank.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapperClass(SortingMapper.class);
        job.setReducerClass(SortingReducer.class);
        //no. of reduce tasks equal 1 to enforce global sorting
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        return job.waitForCompletion(true);
    }

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

                // be sure to read the next line otherwise you'll get an infinite loop
                line = br.readLine();
            }
        } finally {
            // you should close out the BufferedReader
            br.close();
        }
        //Delete temp directory
        //hdfs.delete(new Path(pathString), true);

        return result;
    }

}
