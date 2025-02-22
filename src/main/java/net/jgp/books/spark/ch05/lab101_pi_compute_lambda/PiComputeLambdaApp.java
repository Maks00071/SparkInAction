package net.jgp.books.spark.ch05.lab101_pi_compute_lambda;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;


public class PiComputeLambdaApp implements Serializable {
    private static final long serialVersionUID = -2548L;
    private static long counter = 0;

    public static void main(String[] args) {
        PiComputeLambdaApp app = new PiComputeLambdaApp();
        app.start(10);
    }

    private final class DartMapper implements MapFunction<Row, Integer> {
        private static final long serialVersionUID = 6451L;

        @Override
        public Integer call(Row row) throws Exception {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;

            counter++;
            if (counter % 100_000 == 0) {
                System.out.println("" + counter + " darts throw so far");
            }
            return (x * x + y * y <= 1) ? 1 : 0;
        }
    }

    private final class DartReducer implements ReduceFunction<Integer> {
        private static final long serialVersionUID = 65451L;

        @Override
        public Integer call(Integer x, Integer y) throws  Exception {
            return x + y;
        }
    }


    private void start(int slice) {

        int numberOfThrows = 100_000 * slice;

        System.out.println("About to throw " + numberOfThrows
                + " darts, ready? Stay away from the target!");

        long t0 = System.currentTimeMillis();

        SparkSession spark = SparkSession.builder()
                .appName("Spark Pi with lambda")
                .master("local[*]")
                .getOrCreate();

        long t1 = System.currentTimeMillis();
        System.out.println("Session initialized in " + (t1 - t0) + " ms");

        List<Integer> listOfThrows = new ArrayList<>(numberOfThrows);
        for (int i = 0; i < numberOfThrows; i++) {
            listOfThrows.add(i);
        }

        Dataset<Row> incrementalDf = spark
                .createDataset(listOfThrows, Encoders.INT())
                .toDF();

        incrementalDf.show(5);

        long t2 = System.currentTimeMillis();
        System.out.println("Initial dataframe built in " + (t2 - t1) + " ms");

        // lambda-методы заменяют сразу два класса (DartMapper и DartReducer)
        Dataset<Integer> dotsDs = incrementalDf
                .map((MapFunction<Row, Integer>) status -> {
                    double x = Math.random() * 2 - 1;
                    double y = Math.random() * 2 - 1;
                    counter++;
                    if (counter % 100_000 == 0) {
                        System.out.println("" + counter + " darts thrown so far");
                    }
                    return (x * x + y * y <= 1) ? 1 : 0;
                }, Encoders.INT());

        long t3 = System.currentTimeMillis();
        System.out.println("Throwing darts done in " + (t3 - t2) + " ms");

        dotsDs.show(5);

        int dartsInCircle = dotsDs.reduce((ReduceFunction<Integer>) (x, y) -> x + y);

        long t4 = System.currentTimeMillis();
        System.out.println("Analysing result in " + (t4 - t3) + " ms");

        System.out.println("Pi is roughly " + 4.0 * dartsInCircle / numberOfThrows);
    }

    /*
    Session initialized in 2404 ms

    incrementalDf
    +-----+
    |value|
    +-----+
    |    0|
    |    1|
    |    2|
    |    3|
    |    4|
    +-----+
    only showing top 5 rows

    Initial dataframe built in 4985 ms
    Throwing darts done in 237 ms

    dotsDs
    +-----+
    |value|
    +-----+
    |    0|
    |    1|
    |    1|
    |    1|
    |    1|
    +-----+
    only showing top 5 rows

    100058 darts thrown so far
    200082 darts thrown so far
    300000 darts thrown so far
    400000 darts thrown so far
    500000 darts thrown so far
    600000 darts thrown so far
    700000 darts thrown so far
    800000 darts thrown so far
    900000 darts thrown so far
    Analysing result in 6694 ms
    Pi is roughly 3.1416
     */
}



































