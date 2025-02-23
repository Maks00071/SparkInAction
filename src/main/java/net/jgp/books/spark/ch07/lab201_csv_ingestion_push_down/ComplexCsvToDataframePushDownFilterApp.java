package net.jgp.books.spark.ch07.lab201_csv_ingestion_push_down;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class ComplexCsvToDataframePushDownFilterApp {

    public static void main(String[] args) {
        ComplexCsvToDataframePushDownFilterApp app = new ComplexCsvToDataframePushDownFilterApp();
        app.start();
    }

    /**
     * The processing code
     * <p>
     * {1} - push down filter (условие фильтрации на источнике)
     */
    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV to Dataframe")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "*")
                .option("inferSchema", true)
                .load("data/ch07/books.csv")
                .filter("authorId = 1");                // {1}

        System.out.println("Excerpt of the dataframe content: ");
        // Shows at most 7 rows from the dataframe, with columns as wide as 90
        // characters
        df.show(7, 90);

        System.out.println("Dataframe's schema: ");
        df.printSchema();
    }

    /*
    +---+--------+---------------------------------------------------------------------------------------+-----------+----------------------+
    | id|authorId|                                                                                  title|releaseDate|                  link|
    +---+--------+---------------------------------------------------------------------------------------+-----------+----------------------+
    |  1|       1|                       Fantastic Beasts and Where to Find Them: The Original Screenplay| 11/18/2016|http://amzn.to/2kup94P|
    |  2|       1|  Harry Potter and the Sorcerer's Stone: The Illustrated Edition (Harry Potter; Book 1)| 10/06/2015|http://amzn.to/2l2lSwP|
    |  3|       1|                          The Tales of Beedle the Bard, Standard Edition (Harry Potter)| 12/04/2008|http://amzn.to/2kYezqr|
    |  4|       1|Harry Potter and the Chamber of Secrets: The Illustrated Edition (Harry Potter; Book 2)| 10/04/2016|http://amzn.to/2kYhL5n|
    +---+--------+---------------------------------------------------------------------------------------+-----------+----------------------+

    Dataframe's schema:
    root
     |-- id: integer (nullable = true)
     |-- authorId: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- releaseDate: string (nullable = true)
     |-- link: string (nullable = true)
     */
}



























