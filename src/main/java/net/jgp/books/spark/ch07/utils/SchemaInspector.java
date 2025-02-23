package net.jgp.books.spark.ch07.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Helper class to inspect a dataframe's schema. The printSchema() does not
 * display all the information (Вспомогательный класс для проверки схемы фрейма данных.
 * Функция printSchema() не отображает всю информацию).
 */
public class SchemaInspector {

    public static void print(StructType schema) {
        System.out.println(schema);
    }

    public static void print(String label, Dataset<Row> df) {
        if (label != null) {
            System.out.print(label);
        }
        System.out.println(df.schema());
    }

    public static void print(String label, StructType schema) {
        if (label != null) {
            System.out.print(label);
        }
        System.out.println(schema.json());
    }

}
