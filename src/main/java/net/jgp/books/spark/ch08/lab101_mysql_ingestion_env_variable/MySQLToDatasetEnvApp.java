package net.jgp.books.spark.ch08.lab101_mysql_ingestion_env_variable;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;


public class MySQLToDatasetEnvApp {

    public static void main(String[] args) {
        MySQLToDatasetEnvApp app = new MySQLToDatasetEnvApp();
        app.start();
    }

    /**
     * {1} - получаем значения переменных окружения из системы и преобразуем их в байты
     * {2} - неявная передача пароля
     * {3} - уничтожаем пароль
     */
    private void start() {

        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using a JDBC Connection")
                .master("local[*]")
                .getOrCreate();

        // Using Properties
        Properties props = new Properties();
        props.put("user", "root");

        byte[] password = System.getenv("DB_PASSWORD").getBytes();          // {1}
        props.put("password", new String(password));                        // {2}
        password = null;                                                          // {3}
        props.put("useSSL", "false");

        Dataset<Row> df = spark.read()
                .jdbc("jdbc:mysql://localhost:3306/sakila?serverTimezone=EST", "actor", props);
        df = df.orderBy(df.col("last_name"));

        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " record(s).");
    }
}
