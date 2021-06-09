# PageRank Hadoop

[![Hits](https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fgithub.com%2Fedofazza%2FPageRank-Hadoop&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%23E7E7E7&title=hits&edge_flat=false)](https://hits.seeyoufarm.com)

Implementation of the MapReduce PageRank algorithm using the Hadoop framework in Java. The documentation for this project can be found [here](documentation/latex/pagerankDocumentation.pdf).

## How to run the algorithm
`hadoop jar <app Jar> it.unipi.hadoop.pagerank.PageRank <input file> <output> <number of iterations>`

## Input file
We have tested the algorithm with three input files. 

The first file contains pages from the Simple English Wikipedia. It is a pre-processed version of the Simple Wikipedia corpus in which the pages are stored in an XML format. Each page of Wikipedia is represented in XML as follows:

    <title>page name</title>
        ...
    <revisionoptionalVal="xxx">
            ...
            <textoptionalVal="yyy">page content</text>
            ...
    </revision>

The pages have been "flattened" to be represented on a single line. The body text of the page also has all new lines converted to spaces to ensure it stays on one line in this representation. Links to other Wikipedia articles are of the form [[page name]] and **we considered only links in the _text_ section**.

The other two files contain synthetic datasets we created using the same XML structure with 5000 and 10000 pages, respectively.

All XML files can be found [here](data/).
