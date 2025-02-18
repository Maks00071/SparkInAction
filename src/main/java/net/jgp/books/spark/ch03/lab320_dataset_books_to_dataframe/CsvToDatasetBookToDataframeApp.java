package net.jgp.books.spark.ch03.lab320_dataset_books_to_dataframe;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

import java.io.Serializable;
import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.jgp.books.spark.ch03.x.model.Book;


/**
 * This example will read a CSV-file, ingest it in a dataframe,
 * convert the dataframe to a dataset, and vice versa
 * {C1} - при использовании преобразований и отображений (maps) многи объекты необходимо
 * сделать сериализуемыми (Serializable)
 *
 * @author jpg
 */
public class CsvToDatasetBookToDataframeApp implements Serializable {           // {C1}

    private static final long serialVersionUID = -1L;

    /**
     * This is a mapper class that will convert a Row to an instance of Book.
     * You have full control over it - isn't it great that sometimes you have control?
     * <p>
     * {1} - создаем новый экземпляр книги для каждой записи. Эти объекты не
     * получают преимуществ от оптимизации Tungsten
     * {2} - простое извлечение из объекта-строки (типа Row) в POJO аналогично
     * обработке JDBC ResultSet
     * {3} - Даты обрабатываюся немного сложнее: необходимо преобразовать строку в дату
     * {4} - Также можно преобразовать дату в статическое поле для улучшения производительности
     *
     * @author jpg
     */
    class BookMapper implements MapFunction<Row, Book> {

        private static final long serialVersionUID = -2L;

        @Override
        public Book call(Row value) throws Exception {

                Book book = new Book();                                         // {1}

                book.setId(value.getAs("id"));                         // {2}
                book.setAuthorId(value.getAs("authorId"));             // {2}
                book.setTitle(value.getAs("title"));                   // {2}
                book.setLink(value.getAs("link"));                     // {2}


                String dateAsString = value.getAs("releaseDate");      // {3}

                if (dateAsString != null) {
                    SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");   // {4}
                    book.setReleaseDate(parser.parse(dateAsString));
                }

            return book;
        }
    }

    /**
     * It starts here
     *
     * @param args
     */
    public static void main(String[] args) {
        CsvToDatasetBookToDataframeApp app = new CsvToDatasetBookToDataframeApp();
        app.start();
    }

    /**
     * All the work is done here
     * {5} - создаем сеанс
     * {6} - потребление данных в фрейм данных
     * {7} - преобразование фрейма данных в набор данных с использованием функции map()
     * {8} - expr() - выражение, значением которого является сумма года начала деятельности и 1900
     */
    public void start() {

        SparkSession spark = SparkSession.builder()                             // {5}
                .appName("CSV to dataframe to Dataset<Book> and back")
                .master("local[*]")
                .getOrCreate();

        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

        String fileName = "data/books.csv";                                     // {6}

        Dataset<Row> df = spark.read()                                          // {6}
                .format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(fileName);

        System.out.println("*** Books ingested in a dataframe");
        df.show(25);
        df.printSchema();
        System.out.println(df.count());

        //Encoders.bean() c Spark-3.5.0 не работает
        Dataset<Book> bookDs = df.map(new BookMapper(), Encoders.bean(Book.class));  // {7}
        System.out.println("*** Books are in a dataset of books");
        bookDs.show(5, 17);
        bookDs.printSchema();

        Dataset<Row> df2 = bookDs.toDF();

        df2 = df2.withColumn("releaseDateAsString", concat(
                expr("releaseDate.year + 1900"), lit("-"),          // {8}
                expr("releaseDate.month + 1"), lit("-"),
                df2.col("releaseDate.date")));

        df2.show(5);

        df2 = df2.withColumn(
                "releaseDateAsDate",
                to_date(df2.col("releaseDateAsString"), "yyyy-MM-dd"))
                .drop("releaseDateAsString")
                .drop("releaseDate");

        System.out.println("*** Books are back in a dataframe");
        df2.show(5);
        df2.printSchema();

    }

    /*
    При использовании inferSchema=false (опция по умолчанию) будет создан фрейм данных,
    в котором все столбцы являются строками (StringType).
    В зависимости от того, что вы хотите сделать, строки могут не подойти.
    Например, если вы хотите сложить числа из разных столбцов, то эти столбцы должны
    быть числового типа (строки не подойдут).

    При установке inferSchema=true Spark автоматически просканирует файл CSV и определит схему каждого столбца.
    Это требует дополнительного прохода по файлу, что приведёт к замедлению чтения файла с inferSchema равным true.
    Но в результате фрейм данных, скорее всего, будет иметь правильную схему с учётом входных данных.
     */

    /*
    *** Books ingested in a dataframe
    +---+--------+--------------------+-----------+--------------------+
    | id|authorId|               title|releaseDate|                link|
    +---+--------+--------------------+-----------+--------------------+
    |  1|       1|Fantastic Beasts ...|   11/18/16|http://amzn.to/2k...|
    |  2|       1|Harry Potter and ...|    10/6/15|http://amzn.to/2l...|
    |  3|       1|The Tales of Beed...|    12/4/08|http://amzn.to/2k...|
    |  4|       1|Harry Potter and ...|    10/4/16|http://amzn.to/2k...|
    |  5|       2|Informix 12.10 on...|    4/23/17|http://amzn.to/2i...|
    |  6|       2|Development Tools...|   12/28/16|http://amzn.to/2v...|
    |  7|       3|Adventures of Huc...|    5/26/94|http://amzn.to/2w...|
    |  8|       3|A Connecticut Yan...|    6/17/17|http://amzn.to/2x...|
    | 10|       4|Jacques le Fataliste|     3/1/00|http://amzn.to/2u...|
    | 11|       4|Diderot Encyclope...|       null|http://amzn.to/2i...|
    | 12|    null|   A Woman in Berlin|    7/11/06|http://amzn.to/2i...|
    | 13|       6|Spring Boot in Ac...|     1/3/16|http://amzn.to/2h...|
    | 14|       6|Spring in Action:...|   11/28/14|http://amzn.to/2y...|
    | 15|       7|Soft Skills: The ...|   12/29/14|http://amzn.to/2z...|
    | 16|       8|     Of Mice and Men|       null|http://amzn.to/2z...|
    | 17|       9|Java 8 in Action:...|    8/28/14|http://amzn.to/2i...|
    | 18|      12|              Hamlet|     6/8/12|http://amzn.to/2y...|
    | 19|      13|             Pensées| 12/31/1670|http://amzn.to/2j...|
    | 20|      14|Fables choisies, ...|   9/1/1999|http://amzn.to/2y...|
    | 21|      15|Discourse on Meth...|  6/15/1999|http://amzn.to/2h...|
    | 22|      12|       Twelfth Night|      7/1/4|http://amzn.to/2z...|
    | 23|      12|             Macbeth|      7/1/3|http://amzn.to/2z...|
    +---+--------+--------------------+-----------+--------------------+

    root
     |-- id: integer (nullable = true)
     |-- authorId: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- releaseDate: string (nullable = true)
     |-- link: string (nullable = true)

    22
    *** Books are in a dataset of books
    +--------+---+--------------------+--------------------+--------------------+
    |authorId| id|                link|         releaseDate|               title|
    +--------+---+--------------------+--------------------+--------------------+
    |       1|  1|http://amzn.to/2k...|{18, 0, 0, 10, 0,...|Fantastic Beasts ...|
    |       1|  2|http://amzn.to/2l...|{6, 0, 0, 9, 0, 1...|Harry Potter and ...|
    |       1|  3|http://amzn.to/2k...|{4, 0, 0, 11, 0, ...|The Tales of Beed...|
    |       1|  4|http://amzn.to/2k...|{4, 0, 0, 9, 0, 1...|Harry Potter and ...|
    |       2|  5|http://amzn.to/2i...|{23, 0, 0, 3, 0, ...|Informix 12.10 on...|
    |       2|  6|http://amzn.to/2v...|{28, 0, 0, 11, 0,...|Development Tools...|
    |       3|  7|http://amzn.to/2w...|{26, 0, 0, 4, 0, ...|Adventures of Huc...|
    |       3|  8|http://amzn.to/2x...|{17, 0, 0, 5, 0, ...|A Connecticut Yan...|
    |       4| 10|http://amzn.to/2u...|{1, 0, 0, 2, 0, 9...|Jacques le Fataliste|
    |       4| 11|http://amzn.to/2i...|                null|Diderot Encyclope...|
    |       0| 12|http://amzn.to/2i...|{11, 0, 0, 6, 0, ...|   A Woman in Berlin|
    |       6| 13|http://amzn.to/2h...|{3, 0, 0, 0, 0, 1...|Spring Boot in Ac...|
    |       6| 14|http://amzn.to/2y...|{28, 0, 0, 10, 0,...|Spring in Action:...|
    |       7| 15|http://amzn.to/2z...|{29, 0, 0, 11, 0,...|Soft Skills: The ...|
    |       8| 16|http://amzn.to/2z...|                null|     Of Mice and Men|
    |       9| 17|http://amzn.to/2i...|{28, 0, 0, 7, 0, ...|Java 8 in Action:...|
    |      12| 18|http://amzn.to/2y...|{8, 0, 0, 5, 0, 1...|              Hamlet|
    |      13| 19|http://amzn.to/2j...|{31, 0, 0, 11, 0,...|             Pensées|
    |      14| 20|http://amzn.to/2y...|{1, 0, 0, 8, 0, 9...|Fables choisies, ...|
    |      15| 21|http://amzn.to/2h...|{15, 0, 0, 5, 0, ...|Discourse on Meth...|
    +--------+---+--------------------+--------------------+--------------------+
    only showing top 20 rows

    root
     |-- authorId: integer (nullable = true)
     |-- id: integer (nullable = true)
     |-- link: string (nullable = true)
     |-- releaseDate: struct (nullable = true)
     |    |-- date: integer (nullable = true)
     |    |-- hours: integer (nullable = true)
     |    |-- minutes: integer (nullable = true)
     |    |-- month: integer (nullable = true)
     |    |-- seconds: integer (nullable = true)
     |    |-- time: long (nullable = true)
     |    |-- year: integer (nullable = true)
     |-- title: string (nullable = true)
     */

    /* {8}
    +--------+---+--------------------+--------------------+--------------------+-------------------+
    |authorId| id|                link|         releaseDate|               title|releaseDateAsString|
    +--------+---+--------------------+--------------------+--------------------+-------------------+
    |       1|  1|http://amzn.to/2k...|{18, 0, 0, 10, 0,...|Fantastic Beasts ...|         2016-11-18|
    |       1|  2|http://amzn.to/2l...|{6, 0, 0, 9, 0, 1...|Harry Potter and ...|          2015-10-6|
    |       1|  3|http://amzn.to/2k...|{4, 0, 0, 11, 0, ...|The Tales of Beed...|          2008-12-4|
    |       1|  4|http://amzn.to/2k...|{4, 0, 0, 9, 0, 1...|Harry Potter and ...|          2016-10-4|
    |       2|  5|http://amzn.to/2i...|{23, 0, 0, 3, 0, ...|Informix 12.10 on...|          2017-4-23|
    +--------+---+--------------------+--------------------+--------------------+-------------------+
     */

    /*
    *** Books are back in a dataframe
    +--------+---+--------------------+--------------------+-----------------+
    |authorId| id|                link|               title|releaseDateAsDate|
    +--------+---+--------------------+--------------------+-----------------+
    |       1|  1|http://amzn.to/2k...|Fantastic Beasts ...|       2016-11-18|
    |       1|  2|http://amzn.to/2l...|Harry Potter and ...|       2015-10-06|
    |       1|  3|http://amzn.to/2k...|The Tales of Beed...|       2008-12-04|
    |       1|  4|http://amzn.to/2k...|Harry Potter and ...|       2016-10-04|
    |       2|  5|http://amzn.to/2i...|Informix 12.10 on...|       2017-04-23|
    +--------+---+--------------------+--------------------+-----------------+
    only showing top 5 rows

    root
     |-- authorId: integer (nullable = true)
     |-- id: integer (nullable = true)
     |-- link: string (nullable = true)
     |-- title: string (nullable = true)
     |-- releaseDateAsDate: date (nullable = true)
     */

}












































