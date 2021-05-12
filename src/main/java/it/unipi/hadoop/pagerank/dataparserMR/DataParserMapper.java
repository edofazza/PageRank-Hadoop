package it.unipi.hadoop.pagerank.dataparserMR;

import it.unipi.hadoop.pagerank.page.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataParserMapper extends Mapper<Object, Text, Text, TextArray> {

    // reuse the writable objects
    private final Text outputKey = new Text();
    private final TextArray outputValue = new TextArray();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String title = getTitleFromDocument(value.toString());
        outputKey.set(title);
        outputValue.set(getOutgoingLinksFromDocument(getTextFromDocument(value.toString())));
        context.write(outputKey, outputValue);
    }

    private String getTitleFromDocument (String document)
    {
        String initialString = "<title>";
        // document.indexOf() returns the index of the first character
        return document.substring(
                document.indexOf(initialString) + initialString.length(), // I need to sum the length of the string
                document.indexOf("</title>"));
    }

    private String getTextFromDocument (String document)
    {
        String initialString = "<text xml:space=\"preserve\">";
        return document.substring(
                document.indexOf(initialString) + initialString.length(),
                document.indexOf("</text>"));
    }

    private Text[] getOutgoingLinksFromDocument (String text)
    {
        List<Text> outgoingLinks = new ArrayList<>();
        int i=0;
        while (true)
        {
            String initialString = "[[";
            int start = text.indexOf(initialString, i); // Starting from i
            if (start == -1) break;
            int end = text.indexOf("]]", start); // Starting from start
            outgoingLinks.add(new Text(
                    text.substring(start + initialString.length(), end))
            );
            i = end + 1; // Advance i for the next iteration
        }
        Text[] arrayOfLinks = new Text[outgoingLinks.size()];
        outgoingLinks.toArray(arrayOfLinks);
        return arrayOfLinks;
    }
}
