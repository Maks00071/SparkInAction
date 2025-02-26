package net.jgp.books.spark.ch07.lab700_text_ingestion;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class TextToDataframeApp {

    public static void main(String[] args) {
        TextToDataframeApp app = new TextToDataframeApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Text to Dataframe")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("text")
                .load("data/ch07/romeo-juliet-pg1777.txt");

        System.out.println("******************** OUTPUT ********************\n");
        df.show(10, 100);
        df.printSchema();

    }

    /*
    ******************** OUTPUT ********************

    +-----------------------------------------------------------------+
    |                                                            value|
    +-----------------------------------------------------------------+
    |                                                                 |
    |            This Etext file is presented by Project Gutenberg, in|
    |  cooperation with World Library, Inc., from their Library of the|
    | Future and Shakespeare CDROMS.  Project Gutenberg often releases|
    |                Etexts that are NOT placed in the Public Domain!!|
    |                                                                 |
    | *This Etext has certain copyright implications you should read!*|
    |                                                                 |
    |       <<THIS ELECTRONIC VERSION OF THE COMPLETE WORKS OF WILLIAM|
    |SHAKESPEARE IS COPYRIGHT 1990-1993 BY WORLD LIBRARY, INC., AND IS|
    +-----------------------------------------------------------------+
    only showing top 10 rows

    root
     |-- value: string (nullable = true)
     */
}
