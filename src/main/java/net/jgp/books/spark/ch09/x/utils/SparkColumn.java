package net.jgp.books.spark.ch09.x.utils;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Simple annotation to extend and ease the Javabean metadata when
 * converting to a Spark column in a dataframe
 * <p>
 * {1} - перезапись (замещение) имени столбца <br>
 * {2} - перезапись (замещение) типа столбца (!Spark не будет выполнять преобразование данных автоматически!) <br>
 * {3} - установка свойства недопустимости нулевого значения, т.к. в этом случае не существует
 * способа логического вывода из JavaBean <br>
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface SparkColumn {

    /**
     * The name of the column can be overriden
     */
    String name() default "";                           // {1}

    /**
     * Forces the data type of the column
     */
    String type() default "";                           // {2}

    /**
     * Forces the required/nullable property
     */
    boolean nullable() default true;                    // {3}
}
