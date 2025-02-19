package net.jgp.books.spark.ch04.lab200_transformation_and_action;

import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Scanner;


public class TransformationAndActionApp {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        try {
            System.out.print("Please input a mode: ");
            String mode = scanner.nextLine();
            TransformationAndActionApp app = new TransformationAndActionApp();
            app.start(mode);

        } catch (Exception ex) {
            ex.printStackTrace();

        } finally {
            scanner.close();
        }
    }

    /**
     * Основной метод
     *
     * @param mode метод обработки набора данных <br>
     *             {noop} - без операции/преобразования <br>
     *             {col} - создание столбцов <br>
     *             {full} - полный процесс <br>
     *             {1} - настройка тамймера
     *             {2} - время создание сеанса
     *             {3} - создаем ссылку на фрейм данных, чтобы использовать ее при копировании
     *             {4} - время на считывание CSV-файла и создания фрейма данных
     *             {5} - через цикл увеличиваем размер набора данных
     *             {6} - время на создание набора данных большего размера
     *             {7} - действие аккумуляции
     */
    private void start(String mode) {

        // Step 1 - Creates a session on a local master
        System.out.println("******************** mode=" + mode + " ********************");
        long t0 = System.currentTimeMillis();                                        // {1}

        SparkSession spark = SparkSession.builder()
                .appName("Analysing Catalyst's behavior")
                .master("local[*]")
                .getOrCreate();

        long t1 = System.currentTimeMillis();
        long t1minust0 = (t1 - t0);
        System.out.println("1. Creating a spark session .......... " + t1minust0);   // {2}

        // Step 2 - Reads a CSV file with header, stores it in a dataframe
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/ch04/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");

        Dataset<Row> initalDf = df;                                                 // {3}

        long t2 = System.currentTimeMillis();
        long t2minust1 = (t2 - t1);
        System.out.println("2. Loading initial dataset ........... " + t2minust1);    // {4}

        // Step 3 - Build a bigger dataset
        System.out.println("   df.count: ......................... " + df.count());

        for (int i = 0; i < 60; i++) {                                              // {5}
            df = df.union(initalDf);
        }
        long t3 = System.currentTimeMillis();
        System.out.println("   df.count: ......................... " + df.count());
        long t3minust2 = (t3 - t2);
        System.out.println("3. Building full dataset ............. " + t3minust2);        // {6}

        // Step 4 - Cleanup. preparation
        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");
        long t4 = System.currentTimeMillis();
        long t4minust3 = (t4 - t3);
        System.out.println("4. Clean-up .......................... " + t4minust3);

        // Step 5 - Transformation
        /*
         если задан режим "noop", то пропустить все преобразования, иначе создать
        новые столбцы.
         если задан режим "full", то удалить только что созданные новые столбцы
         */
        if (mode.compareToIgnoreCase("noop") != 0) {
            df = df.withColumn("avg", expr("(lcl+ucl)/2"))
                    .withColumn("lcl2", df.col("lcl"))
                    .withColumn("ucl2", df.col("ucl"));

            if (mode.compareToIgnoreCase("full") == 0) {
                df = df.drop(df.col("avg"))
                        .drop(df.col("lcl2"))
                        .drop(df.col("ucl2"));
            }
        }
        long t5 = System.currentTimeMillis();
        long t5minust4 = (t5 - t4);
        System.out.println("5. Transformations ................... " + t5minust4);

        // Step 6 - Action -- здесь Spark начинает работать!!! Все выше - это набор правил, как ему работать
        df.collect();                                                               // {7}
        long t6 = System.currentTimeMillis();
        System.out.println("6. Final action ...................... " + (t6 - t5));
        System.out.println("");
        System.out.println("# of records ......................... " + df.count());
        System.out.println("# totalTime: ......................... " + (t6 - t0)/1000 + " sec");
    }

    /*
    ******************** mode=noop ********************
    1. Creating a spark session .......... 2262
    2. Loading initial dataset ........... 3327
       df.count: ......................... 40781
       df.count: ......................... 2487641
    3. Building full dataset ............. 837
    4. Clean-up .......................... 1471
    5. Transformations ................... 0
    6. Final action ...................... 8768

    # of records ......................... 2487641
    # totalTime: ......................... 19 sec
     */

    /*
    ******************** mode=col ********************
    1. Creating a spark session .......... 2386
    2. Loading initial dataset ........... 3398
       df.count: ......................... 40781
       df.count: ......................... 2487641
    3. Building full dataset ............. 849
    4. Clean-up .......................... 1452
    5. Transformations ................... 77
    6. Final action ...................... 13456

    # of records ......................... 2487641
    # totalTime: ......................... 20 sec
     */

    /*
    ******************** mode=full ********************
    1. Creating a spark session .......... 2277
    2. Loading initial dataset ........... 3350
       df.count: ......................... 40781
       df.count: ......................... 2487641
    3. Building full dataset ............. 845
    4. Clean-up .......................... 1432
    5. Transformations ................... 82
    6. Final action ...................... 12656

    # of records ......................... 2487641
    # totalTime: ......................... 21 sec
     */


}













































