package net.jgp.books.spark.ch05.lab100_pi_compute;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;              // {1}
import org.apache.spark.api.java.function.ReduceFunction;           // {2}
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// в данной лабораторной работе мы реализуем MapReduce

public class PiComputeApp implements Serializable {

    private static final long serialVersionUID = -1546L;
    private static long counter = 0;

    public static void main(String[] args) {
        PiComputeApp app = new PiComputeApp();
        app.start(10);
    }

    /**
     * Mapper class, creates the map of dots
     * <p>
     * {m1} - класс отображения
     * {m2} - метание дротиков выполняется случайным образом, x и y - координаты точки попадания
     * {m3} - счетчик точек (бросков)
     * {m4} - 1, если точка внутри круга (x^2 + y^2) <= 1, иначе 0
     */
    private final class DartMapper implements MapFunction<Row, Integer> {       // {m1}
        private static final long serialVersionUID = 38446L;

        @Override
        public Integer call(Row row) throws Exception {
            // пишем тело метода
            double x = Math.random() * 2 - 1;                                   // {m2}
            double y = Math.random() * 2 - 1;                                   // {m2}

            counter++;                                                          // {m3}
            if (counter % 100_000 == 0) {
                System.out.println("" + counter + " darts throw so far");
            }
            return (x * x + y * y <= 1) ? 1 : 0;                                // {m4}
        }
    }

    /**
     * Reducer class, reduces the map of dots
     * <p>
     * {r1} - объект свертки суммирует результаты
     * {r2} - возврат суммы всех результатов бросков
     */
    private final class DartReducer implements ReduceFunction<Integer> {         // {r1}
        private static final long serialVersionUID = 12859L;

        @Override
        public Integer call(Integer x, Integer y) throws Exception {            // {r1}
            return x + y;                                                       // {r2}
        }
    }


    /**
     * Точка запустка приложения
     * <p>
     * {1} - используется для объекта отображения
     * {2} - используется для объекта свертки
     * {3} - используем кол-во фрагментов как множитель
     *
     * @param slices кол-во фрагментов
     */
    private void start(int slices) {
        int numberOfThrows = 100_000 * slices;                    // {3}
        System.out.println("About to throw " + numberOfThrows
                + " darts, ready? Stay away from the target!");

        long t0 = System.currentTimeMillis();

        SparkSession spark = SparkSession.builder()
                .appName("Spark Pi")
                .master("local[*]")
                .getOrCreate();

        long t1 = System.currentTimeMillis();
        System.out.println("Session initialized in " + (t1 - t0) + " ms");

        // из списка целых значений создается набор данных, который преобразовывается во фрейм данных
        List<Integer> listOfThrows = new ArrayList<>(numberOfThrows);

        for (int i = 0; i < numberOfThrows; i++) {
            listOfThrows.add(i);
        }

        Dataset<Row> incrementalDf = spark
                .createDataset(listOfThrows, Encoders.INT())
                .toDF();

        long t2 = System.currentTimeMillis();
        System.out.println("Initial dataframe built in " + (t2 - t1) + " ms");

        /*
        каждая строка фрейма данных incrementalDf передается в экземпляр DartMapper,
        который находится на всех физических узлах кластера
         */
        Dataset<Integer> dartsDs = incrementalDf.map(new DartMapper(), Encoders.INT());
        long t3 = System.currentTimeMillis();
        System.out.println("Throwing darts done in " + (t3 - t2) + " ms");

        /*
        операция свертки (reduce) возвращает результат: кол-во дротиков, попавших во
        внутреннюю область круга
         */
        int dartsInCiecle = dartsDs.reduce(new DartReducer());
        long t4 = System.currentTimeMillis();
        System.out.println("Analyzing result in " + (t4 - t3) + " ms");

        // выводим приближенного оценочного значения числа Pi
        System.out.println("Pi is roughly " + 4.0 * dartsInCiecle / numberOfThrows);

        spark.stop();
    }

    /*
    Session initialized in 2340 ms
    Initial dataframe built in 3547 ms
    Throwing darts done in 188 ms
    118064 darts throw so far
    200068 darts throw so far
    300098 darts throw so far
    400002 darts throw so far
    500000 darts throw so far
    600000 darts throw so far
    700001 darts throw so far
    800001 darts throw so far
    900000 darts throw so far
    Analyzing result in 4719 ms
    Pi is roughly 3.140228
     */
}










































