package it.unipi.hadoop.pagerank.dataparserMR;

import it.unipi.hadoop.pagerank.model.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner used in the parsing phase for dividing the intermediate key in two different subset
 * The keys "" will go always on Reducer 0
 * The others keys will be divided in reducers
 * The key "" will be always the first processed in reducer 0, so will stay on the first line
 */
public class ParserPartitioner extends Partitioner<Text, TextArray> {
    public int getPartition(Text key, TextArray value, int numReduceTasks){
        if (numReduceTasks > 1) // If I have more the one reducer
        {
            if (!key.toString().equals("")) // If it is not the special key ""
                // choose a partition using a criteria based on hash value
                return ((key.hashCode() & Integer.MAX_VALUE) % (numReduceTasks));
        }
        return 0; // otherwise send to Reducer 0
    }
}

