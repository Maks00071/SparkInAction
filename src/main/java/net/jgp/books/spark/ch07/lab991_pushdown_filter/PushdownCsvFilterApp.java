package net.jgp.books.spark.ch07.lab991_pushdown_filter;

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// Режим сохранения (SaveMode) используется для указания ожидаемого
// поведения при сохранении фрейма данных в источнике данных
import org.apache.spark.sql.SaveMode;


public class PushdownCsvFilterApp {

    enum Mode {
        NO_FILTER, FILTER
    }


    public static void main(String[] args) {
        PushdownCsvFilterApp app = new PushdownCsvFilterApp();
//        app.buildBigCsvFile();
        app.start(Mode.FILTER);
    }

    /**
     * Метод формирует большой CSV-файл
     * <p>
     * {1} - если true, то включить передачу фильтра в источник данных CSV (true - по-умолчанию)<br>
     * {2} - открываем канал на чтение из файла (формат - CSV)<br>
     * {3} - наличие заголовка у файла<br>
     * {4} - При установке "inferSchema=true" Spark автоматически просканирует файл CSV
     * и определит схему каждого столбца. При "inferSchema=true" тип каждого атрибута будет String<br>
     * {5} - увеличим исходного размер CSV-файла в 10 раз (через union с самим собой)<br>
     * {6} - coalesce используется для уменьшения количества разделов (партиций) до указанного количества.
     * В качестве параметра используется номер раздела (кол-во партиций).<br>
     * {7} - открываем канал на запись в файл<br>
     * {8} - записываем в файл CSV-формата<br>
     * {9} - заголовок CSV-файла сохраняем<br>
     * {10} - Режим сохранения файла:<br>
     * <b>SaveMode.Append</b> - Режим добавления (Append) означает, что при сохранении фрейма
     * данных в источник данных, если данные/таблица уже существуют, ожидается, что содержимое фрейма
     * анных будет добавлено к существующим данным.<br>
     * <b>SaveMode.ErrorIfExists</b> - Режим ErrorIfExists означает, что при сохранении фрейма данных
     * в источнике данных, если данные уже существуют, ожидается возникновение исключения.<br>
     * <b>SaveMode.Ignore</b> - Режим игнорирования означает, что при сохранении фрейма данных в источник данных,
     * если данные уже существуют, ожидается, что операция сохранения не сохранит содержимое
     * фрейма данных и не изменит существующие данные.<br>
     * <b>SaveMode.Overwrite</b> - Режим перезаписи означает, что при сохранении фрейма данных в источнике данных,
     * если данные/таблица уже существуют, ожидается, что существующие данные будут
     * перезаписаны содержимым фрейма данных.<br>
     * {11} - указываем, куда надо сохранить CSV-файл
     * {12} - останавливаем Spark-сессию
     */
    private void buildBigCsvFile() {

        System.out.println("Build big CSV file");
        SparkSession spark = SparkSession.builder()
                .appName("Pushdown CSV filter")
                .config("spark.sql.csv.filterPushdown", false)                      // {1}
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().hadoopConfiguration()
                .setClass("fs.file.impl", BareLocalFileSystem.class, FileSystem.class);

        System.out.println("Read initial CSV");
        Dataset<Row> df = spark.read()                                              // {2}
                .format("csv")
                .option("header", true)                                             // {3}
                .option("inferSchema", true)                                        // {4}
                .load("data/ch07/VideoGameSales/vgsales.csv");

        System.out.println("Increasing number of record 10x");                      // {5}
        for (int i = 0; i < 10; i++) {
            System.out.println("Increasing number of record, step #" + i);
            df = df.union(df);
        }

        System.out.println("Saving big CSV file");
        df = df.coalesce(1); // уменьшим размер до 1- партиции         // {6}
        df.write()                                                                 // {7}
                .format("csv")                                              // {8}
                .option("header", true)                                            // {9}
                .mode(SaveMode.Overwrite)                                //  {10}
                .save("data/ch07/VideoGameSales/tmp/vgasales/csv");          //  {11}
        spark.stop();                                                             //  {12}

        System.out.println("Big CSV file ready");
    }

    /**
     * Метод возвращает Dataframe с требуемой фильтрацией
     *
     * @param spark
     * @return Dataset<Row>
     */
    private Dataset<Row> ingestionWithFilter(SparkSession spark) {

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/ch07/VideoGameSales/tmp/vgasales/csv/*.csv")
                .filter("Platform = 'Wii'");
        return df;
    }

    /**
     * Метод возвращает Dataframe как есть, без фильтрации
     *
     * @param spark
     * @return Dataset<Row>
     */
    private Dataset<Row> ingestionWithoutFilter(SparkSession spark) {

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/ch07/VideoGameSales/tmp/vgasales/csv/*.csv");
        return df;
    }

    /**
     * {1} - .explain(mode) принимает следующие параметры mode:<br>
     * <b>extended</b> - Создает проанализированный логический план, проанализированный логический план,
     * оптимизированный логический план и физический план<br>
     * <b>codegen</b> - Генерирует код для инструкции, если таковая имеется, и физический план<br>
     * <b>cost</b> - Если доступна статистика узла плана, генерируется логический план и статистика<br>
     * <b>formatted</b> - Создает два раздела: схему физического плана и сведения об узле<br>
     * <b>statement</b> - Задает инструкцию SQL
     *
     */
    private void start(Mode filter) {

        SparkSession spark = null;
        Dataset<Row> df = null;

        long t0 = System.currentTimeMillis();

        switch (filter) {
            case NO_FILTER:
                spark = SparkSession.builder()
                        .appName("Pushdown CSV filter")
                        .config("spark.sql.csv.filterPushdown.enabled", false) // фильтр на источнике выключен
                        .master("local[*]")
                        .getOrCreate();

                spark.sparkContext().hadoopConfiguration()
                        .setClass("fs.file.impl", BareLocalFileSystem.class, FileSystem.class);

                System.out.println("Using Apache Spark v" + spark.version());
                df = ingestionWithoutFilter(spark);
                break;

            case FILTER:
                spark = SparkSession.builder()
                        .appName("Pushdown CSV filter")
                        .config("spark.sql.csv.filterPushdown.enabled", true) // фильтр на источнике включен
                        .master("local[*]")
                        .getOrCreate();

                spark.sparkContext().hadoopConfiguration()
                        .setClass("fs.file.impl", BareLocalFileSystem.class, FileSystem.class);

                System.out.println("Using Apache Spark v" + spark.version());
                df = ingestionWithFilter(spark);
                break;
        }

        df.explain("formatted");        // {1}
        df.write().format("parquet").mode(SaveMode.Overwrite)
                .save("data/ch07/VideoGameSales/tmp/vgasales/" + filter);
        long t1 = System.currentTimeMillis();
        spark.stop();

        System.out.println("**************************** OUTPUT ****************************");
        System.out.println("Operation " + filter + " tool " + (t1 - t0)/1000 + " s");
    }

    /*
    Read initial CSV
    Increasing number of record 10x
    Increasing number of record, step #0
    Increasing number of record, step #1
    Increasing number of record, step #2
    Increasing number of record, step #3
    Increasing number of record, step #4
    Increasing number of record, step #5
    Increasing number of record, step #6
    Increasing number of record, step #7
    Increasing number of record, step #8
    Increasing number of record, step #9
    Saving big CSV file
    Big CSV file ready

    Using Apache Spark v3.3.2
    == Physical Plan ==
    Scan csv  (1)


    (1) Scan csv
    Output [11]: [Rank#17, Name#18, Platform#19, Year#20, Genre#21, Publisher#22, NA_Sales#23, EU_Sales#24, JP_Sales#25, Other_Sales#26, Global_Sales#27]
    Batched: false
    Location: InMemoryFileIndex [file:/C:/Users/corys/IdeaProjects/SparkInAction/data/ch07/VideoGameSales/tmp/vgasales/csv/part-00000-57fa3773-effe-475d-adb4-100dedd4a177-c000.csv]
    ReadSchema: struct<Rank:int,Name:string,Platform:string,Year:string,Genre:string,Publisher:string,NA_Sales:double,EU_Sales:double,JP_Sales:double,Other_Sales:double,Global_Sales:double>


    **************************** OUTPUT ****************************
    Operation NO_FILTER tool 29 s
     */

    /*
    Using Apache Spark v3.3.2
    == Physical Plan ==
    * Filter (2)
    +- Scan csv  (1)


    (1) Scan csv
    Output [11]: [Rank#17, Name#18, Platform#19, Year#20, Genre#21, Publisher#22, NA_Sales#23, EU_Sales#24, JP_Sales#25, Other_Sales#26, Global_Sales#27]
    Batched: false
    Location: InMemoryFileIndex [file:/C:/Users/corys/IdeaProjects/SparkInAction/data/ch07/VideoGameSales/tmp/vgasales/csv/part-00000-57fa3773-effe-475d-adb4-100dedd4a177-c000.csv]
    PushedFilters: [IsNotNull(Platform), EqualTo(Platform,Wii)]
    ReadSchema: struct<Rank:int,Name:string,Platform:string,Year:string,Genre:string,Publisher:string,NA_Sales:double,EU_Sales:double,JP_Sales:double,Other_Sales:double,Global_Sales:double>

    (2) Filter [codegen id : 1]
    Input [11]: [Rank#17, Name#18, Platform#19, Year#20, Genre#21, Publisher#22, NA_Sales#23, EU_Sales#24, JP_Sales#25, Other_Sales#26, Global_Sales#27]
    Condition : (isnotnull(Platform#19) AND (Platform#19 = Wii))


    **************************** OUTPUT ****************************
    Operation FILTER tool 21 s
     */
}











































