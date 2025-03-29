package net.jgp.books.spark.ch09.x.ds.exif;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;    // {1}

import org.apache.spark.sql.SQLContext;                                 // {2}
import org.apache.spark.sql.sources.BaseRelation;                       // {3}
import org.apache.spark.sql.sources.RelationProvider;                   // {4}
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch09.x.extlib.RecursiveExtensionFilteredLister;  // {5}
import net.jgp.books.spark.ch09.x.utils.K;                                  // {6}
import scala.collection.immutable.Map;                                      // {7}


/**
 * Класс создает "отношение" и "наблюдателя" за фотографиями photoLister на основе параметров,
 * переданных в коде приложения
 * <p>
 * {1} - статический метод для преобразования отображения (map) Scala в отображение (map) Java <br>
 * {2} - С версии 2.0 класс был заменён на SparkSession <br>
 * {3} - абстрактный класс, который представляет собой сборку кортежей с известной схемой
 * в контексте Apache Spark и Spark SQL <br>
 * {4} - интерфейс из пакета org.apache.spark.sql.sources, который реализуют объекты,
 * создающие отношения для определённого источника данных <br>
 * {5} - кастомный класс для создания списка файлов для использования <br>
 * {6} - кастомный абстрактный класс констант <br>
 * {7} - отображение контейнер/коллекция, но в реализации Scala <br>
 * {8} - параметры содержатся в отображении Scala <br>
 * {9} - получаем отображение Java <br>
 * {10} - Реализация отношения ExifDirectoryRelation <br>
 * {11} - для отношения ExifDirectoryRelation требуется доступ к контексту SQL Context <br>
 * {12} - Вспомогательный класс RecursiveExtensionFilteredLister по извлечению метаданных <br>
 * {13} - Анализ всех параметров, переданных из приложения, и вызов соответствующего set-метода <br>
 */
public class ExifDirectoryDataSource implements RelationProvider {
    private static Logger log = LoggerFactory.getLogger(ExifDirectoryDataSource.class);

    /**
     * Creates a base relation using the Spark's SQL context and a map of parameters (our options)
     */
    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> params) {             // {8}
        log.debug("-> createRelation()");

        java.util.Map<String, String> optionsAsJavaMap = mapAsJavaMapConverter(params).asJava();    // {9}

        // Creates a specific EXIF relation
        ExifDirectoryRelation br = new ExifDirectoryRelation();                                         // {10}
        br.setSqlContext(sqlContext);                                                                   // {11}

        // Defines the process of acquiring the data through listing files
        RecursiveExtensionFilteredLister photoLister = new RecursiveExtensionFilteredLister();          // {12}

        for (java.util.Map.Entry<String, String> entry : optionsAsJavaMap.entrySet()) {                 // {13}
            String key = entry.getKey().toLowerCase();
            String value = entry.getValue();
            log.debug("[{}] --> [{}]", key, value);

            switch (key) {
                case K.PATH:
                    photoLister.setPath(value);
                    break;

                case K.RECURSIVE:
                    if (value.toLowerCase().charAt(0) == 't') {
                        photoLister.setRecursive(true);
                    } else {
                        photoLister.setRecursive(false);
                    }
                    break;

                case K.LIMIT:
                    int limit;

                    try {
                        limit = Integer.valueOf(value);
                    } catch (NumberFormatException e) {
                        log.error(
                                "Illegal value for limit, expecting a number, got: {}. {}. Ignoring parameter.",
                                value,
                                e.getMessage());
                        limit = -1;
                    }
                    photoLister.setLimit(limit);
                    break;

                case K.EXTENSIONS:
                    String[] extensions = value.split(",");

                    for (int i = 0; i < extensions.length; i++) {
                        photoLister.addExtension(extensions[i]);
                    }
                    break;

                default:
                    log.warn("Unrecognized parameter: [{}].", key);
                    break;
            }
        }

        br.setPhotoLister(photoLister);
        return br;
    }
}




































