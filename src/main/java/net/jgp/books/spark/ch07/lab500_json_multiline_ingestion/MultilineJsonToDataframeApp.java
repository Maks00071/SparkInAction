package net.jgp.books.spark.ch07.lab500_json_multiline_ingestion;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;


public class MultilineJsonToDataframeApp {

    public static void main(String[] args) {
        MultilineJsonToDataframeApp app = new MultilineJsonToDataframeApp();
        app.start();
    }

    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("Multiline JSON to Dataframe")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", true)
                .load("data/ch07/countrytravelinfo.json");

        df.show(5);
        df.printSchema();
    }

    /*
    +-----------------------+-----------------------+--------------------+--------------------+--------+--------------------+------------------------------------+--------------------+---+--------------------------+---------------------+
    |destination_description|entry_exit_requirements|    geopoliticalarea|              health|iso_code|    last_update_date|local_laws_and_special_circumstances| safety_and_security|tag|travel_embassyAndConsulate|travel_transportation|
    +-----------------------+-----------------------+--------------------+--------------------+--------+--------------------+------------------------------------+--------------------+---+--------------------------+---------------------+
    |   <p>The three isla...|   <p>All U.S. citiz...|Bonaire, Sint Eus...|<p>Medical care o...|        |Last Updated: Sep...|                <p>&nbsp;</p><p><...|<p>There are no k...| A1|          <div class="c...| <p><b>Road Condit...|
    |   <p>French Guiana ...|   <p>Visit the&nbsp...|       French Guiana|<p>Medical care w...|      GF|Last Updated: Oct...|                <p><b>Criminal Pe...|<p>French Guiana ...| A2|          <div class="c...| <p><b>Road Condit...|
    |   <p>See the Depart...|   <p><b>Passports a...|       St Barthelemy|<p><b>We do not p...|      BL|Last Updated: Jun...|                <p><b>Criminal Pe...|<p><b>Crime</b>: ...| A3|          <div class="c...| <p><b>Road Condit...|
    |   <p>Read the Depar...|   <p>Upon arrival i...|               Aruba|<p>Access to qual...|      AW|Last Updated: May...|                <p><b>Criminal Pe...|<p>Crimes of oppo...| AA|          <div class="c...| <p><b>Road Condit...|
    |   <p>See the Depart...|   <p><b>Passports a...| Antigua and Barbuda|<p><b>We do not p...|      AG|Last Updated: Jun...|                <p><b>Criminal Pe...|<p><b>Crime</b>:&...| AC|          <div class="c...| <p><b>Road Condit...|
    +-----------------------+-----------------------+--------------------+--------------------+--------+--------------------+------------------------------------+--------------------+---+--------------------------+---------------------+
    only showing top 5 rows

    root
     |-- destination_description: string (nullable = true)
     |-- entry_exit_requirements: string (nullable = true)
     |-- geopoliticalarea: string (nullable = true)
     |-- health: string (nullable = true)
     |-- iso_code: string (nullable = true)
     |-- last_update_date: string (nullable = true)
     |-- local_laws_and_special_circumstances: string (nullable = true)
     |-- safety_and_security: string (nullable = true)
     |-- tag: string (nullable = true)
     |-- travel_embassyAndConsulate: string (nullable = true)
     |-- travel_transportation: string (nullable = true)
     */
}
