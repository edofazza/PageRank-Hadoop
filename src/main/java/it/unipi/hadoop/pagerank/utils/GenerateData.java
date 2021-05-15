package it.unipi.hadoop.pagerank.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class GenerateData {
    private static final String pathSites = "data/siteNames.txt";

    public static void main(String[] args)  {
        List<String> sites = new ArrayList<>();

        // READ SITES
        try (BufferedReader br = new BufferedReader(new FileReader(pathSites));) {
            String line = br.readLine();

            while (line != null) {
                sites.add(line);
                line = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // GENERATE RECORDS
        /*
               <title>    </title>
               <text> [[edge]] </text>
        */
        for (String site: sites) {
            String output = "<title>" + site + "</title><text>";

            int limit = (int) Math.round(Math.random()*5);
            for (int i = 0; i < limit; i++) {
                output += "[[";

                // CHOOSE RANDOM NEIGHBOR
                String tmp;
                do {
                    tmp = sites.get((int) Math.round(Math.random() * sites.size()));
                } while(site.equals(tmp));

                output += tmp + "]]";
            }

            output += "</text>\n";
            try {
                //Files.write(Paths.get("cipherCodeRepo/cipherFavorite/cipherUser"+ nFile + ".txt"), query.getBytes(), StandardOpenOption.APPEND);
                Files.write(Paths.get("data/dataset.txt"), output.getBytes(), StandardOpenOption.APPEND);
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}