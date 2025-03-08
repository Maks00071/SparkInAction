package net.jgp.books.spark.ch08.lab200_informix_dialect;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.DataType;             // {1}
import org.apache.spark.sql.types.DataTypes;            // {2}
import org.apache.spark.sql.types.MetadataBuilder;      // {3}

import scala.Option;


/**
 * Класс-диалект с минимальным необходимым кодом диалекта, позволяющий Spark
 * обмениваться информацией с БД IBM Informix
 * <p>
 * {1} - <b>org.apache.spark.sql.types.DataType</b> — это абстрактный класс, базовый тип всех встроенных
 * типов данных в Spark SQL. Он имеет две основные семьи типов: атомные (внутренний тип для
 * представления типов, которые не являются нулевыми), UDTs, массивы, структуры и карты. <br>
 * {2} - <b>org.apache.spark.sql.types.DataTypes</b> - это класс на Java с методами для доступа к
 * простым или создания сложных типов DataType в Spark SQL, например, массивов и карт. <br>
 * {3} - <b>org.apache.spark.sql.types.MetadataBuilder</b> — это класс для построения метаданных в Spark.
 * С его помощью можно, например, прикрепить произвольный JSON-документ к каждому столбцу
 * для отслеживания происхождения данных, хранения диагностической информации
 * или выполнения разных задач по обогащению данных. <br>
 * {4} - объект JdbcDialect является сериализуемым, поэтому необходим особый уникальный идентификатор для
 * создаваемого класса <br>
 * {5} - canHandle() - метод фильтрации, который позволяет Spark узнать, какой драйвер следует использовать
 * в текущем контексте <br>
 * {6} - сигнатура драйвера JDBC Informix: informix-sqli <br>
 * {7} - преобразование типа данных SQL в тип данных Spark <br>
 * {8} - Spark ничего не знает о типе данных SERIAL, но для Spark это просто целочисленный тип.
 * {9} - Возврат значения в стиле Scala
 */
public class InformixJdbcDialect extends JdbcDialect {
    private static final long serialVersionUID = -672901;   // {4}

    @Override
    public boolean canHandle(String url) {                  // {5}
        return url.startsWith("jdbc:informix-sqli");        // {6}
    }

    @Override
    public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {   // {7}
        if (typeName.toLowerCase().compareTo("serial") == 0) {  // {8}
            return Option.apply(DataTypes.IntegerType);      // {9}
        }
        if (typeName.toLowerCase().compareTo("calendar") == 0) {
            return Option.apply(DataTypes.BinaryType);      // {9}
        }
        if (typeName.toLowerCase().compareTo("calendarpattern") == 0) {
            return Option.apply(DataTypes.BinaryType);      // {9}
        }
        if (typeName.toLowerCase().compareTo("se_metadata") == 0) {
            return Option.apply(DataTypes.BinaryType);      // {9}
        }
        if (typeName.toLowerCase().compareTo("sysbldsqltext") == 0) {
            return Option.apply(DataTypes.BinaryType);      // {9}
        }
        if (typeName.toLowerCase().compareTo("timeseries") == 0) {
            return Option.apply(DataTypes.BinaryType);      // {9}
        }
        if (typeName.toLowerCase().compareTo("st_point") == 0) {
            return Option.apply(DataTypes.BinaryType);      // {9}
        }
        if (typeName.toLowerCase().compareTo("tspartitiondesc_t") == 0) {
            return Option.apply(DataTypes.BinaryType);      // {9}
        }
        return Option.empty();
    }

}









































