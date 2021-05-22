package it.unipi.hadoop.pagerank.dataparserMR;

import it.unipi.hadoop.pagerank.model.TextArray;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mapper used for parsing the pages and retrieves the interesting information
 * KEY_INPUT:       offset of the line read from the file
 * VALUE_INPUT:     one line of the dataset, so one page
 * KEY_OUTPUT:      title of the page (or "" for counting purpose)
 * VALUE_OUTPUT:    array of the outgoing links of this page (or an array with one single element with the counter value)
 * We have decided to implement an In-Mapper combiner, for sending one cumulative counter from each Mapper
 */
public class DataParserMapper extends Mapper<Object, Text, Text, TextArray> {
    private static final Text outputKey = new Text();
    private static final TextArray outputValue = new TextArray();
    private static long counter;

    @Override
    protected void setup(Context context) {
        counter = 0;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // All this operations are made only once for each Mapper, not for each map function
        Text[] texts = new Text[1];
        texts[0] = new Text(String.valueOf(counter)); // the string representation
        outputValue.set(texts);
        context.write(new Text(""), outputValue);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        counter++; // at each call of the map function we increase the counter
        String text = value.toString().replace("\t", " "); // remove all tabs, because we use tabs for separating values
        outputKey.set(getTitleFromPage(text));
        outputValue.set(getOutgoingLinksFromPage(
                getTextFromPage(text))
        );
        context.write(outputKey, outputValue);
    }

    /**
     * Function that retrieves the title of the page
     * @param page  Page in input to the map function
     * @return          Title of the page
     */
    private String getTitleFromPage (String page)
    {
        String initialString = "<title>";
        // document.indexOf() returns the index of the first character
        return page.substring(
                page.indexOf(initialString) + initialString.length(), // I need to sum the length of the string
                page.indexOf("</title>"));
    }

    /**
     * Function that retrieves the Text field of the page, in which there are the links
     * @param page      page to parse
     * @return              The text field
     */
    private String getTextFromPage (String page)
    {
        String initialString = "<text xml:space=\"preserve\">";
        return page.substring(
                page.indexOf(initialString) + initialString.length(),
                page.indexOf("</text>"));
    }

    /**
     * Function that retrieves the outgoing links from the text field of the page
     * @param text      Text field to analyze
     * @return          The list of outgoing links
     */
    private Text[] getOutgoingLinksFromPage (String text)
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
