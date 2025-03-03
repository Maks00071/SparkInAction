package net.jgp.books.spark.ch07.lab920_orc_ingestion;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



public class OrcToDataframeApp {

    public static void main(String[] args) {
        OrcToDataframeApp app = new OrcToDataframeApp();
        app.start();
    }

    /**
     * {1} - используется собственная встроенная реализация для доступа к ORC-файлу, а не реализация Hive
     */
    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("ORC to Dataframe")
                .config("spark.sql.orc.impl", "native")     // {1}
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("orc")
                .load("data/ch07/demo-11-zlib.orc");

        System.out.println("********************** OUTPUT **********************");
        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows");
    }

    /*
    ********************** OUTPUT **********************
    +-----+-----+-----+-------+-----+-----+-----+-----+-----+
    |_col0|_col1|_col2|  _col3|_col4|_col5|_col6|_col7|_col8|
    +-----+-----+-----+-------+-----+-----+-----+-----+-----+
    |    1|    M|    M|Primary|  500| Good|    0|    0|    0|
    |    2|    F|    M|Primary|  500| Good|    0|    0|    0|
    |    3|    M|    S|Primary|  500| Good|    0|    0|    0|
    |    4|    F|    S|Primary|  500| Good|    0|    0|    0|
    |    5|    M|    D|Primary|  500| Good|    0|    0|    0|
    |    6|    F|    D|Primary|  500| Good|    0|    0|    0|
    |    7|    M|    W|Primary|  500| Good|    0|    0|    0|
    |    8|    F|    W|Primary|  500| Good|    0|    0|    0|
    |    9|    M|    U|Primary|  500| Good|    0|    0|    0|
    |   10|    F|    U|Primary|  500| Good|    0|    0|    0|
    +-----+-----+-----+-------+-----+-----+-----+-----+-----+
    only showing top 10 rows

    root
     |-- _col0: integer (nullable = true)
     |-- _col1: string (nullable = true)
     |-- _col2: string (nullable = true)
     |-- _col3: string (nullable = true)
     |-- _col4: integer (nullable = true)
     |-- _col5: string (nullable = true)
     |-- _col6: integer (nullable = true)
     |-- _col7: integer (nullable = true)
     |-- _col8: integer (nullable = true)

    The dataframe has 1920800 rows
     */
}
