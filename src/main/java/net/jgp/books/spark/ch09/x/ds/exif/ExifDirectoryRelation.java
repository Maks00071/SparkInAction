package net.jgp.books.spark.ch09.x.ds.exif;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;                               // {1}
import org.apache.spark.sql.sources.TableScan;                                  // {2}
import org.apache.spark.sql.types.StructType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch09.x.extlib.ExifUtils;
import net.jgp.books.spark.ch09.x.extlib.PhotoMetadata;
import net.jgp.books.spark.ch09.x.extlib.RecursiveExtensionFilteredLister;
import net.jgp.books.spark.ch09.x.utils.Schema;
import net.jgp.books.spark.ch09.x.utils.SparkBeanUtils;


/**
 * Build a relation to return the EXIF data of photos in a directory
 * <p>
 * {1} - Класс (отношение) BaseRelation реализует методы sqlContex() и Schema() <br>
 * {2} - Класс TableScan предоставляет метод buildScan(), необходимый для возврата данных <br>
 * {3} - Класс должен быть сериализуемым, чтобы обеспечить возможность его совместного использования <br>
 * {4} - Следствие свойства сериализуемости - неповторяющийся идентификатор (serialVersionUID)
 * {5} - логгирование
 * {6} - потребуется констекст SQL Context приложения и обязательная реализация get-метода
 * {7} - кеширование схемы, чтобы избежать повторного вычисления (вывода)
 * {8} - предварительно созданный наблюдатель ("слушатель") файлов фотографий
 */
public class ExifDirectoryRelation extends BaseRelation implements Serializable, TableScan {            // {3}

    private static final long serialVersionUID = 4598175080399877334L;                                  // {4}
    private static transient Logger log = LoggerFactory.getLogger(ExifDirectoryRelation.class);   // {5}

    private SQLContext sqlContext;                                                                      // {6}
    private Schema schema = null;                                                                       // {7}
    private RecursiveExtensionFilteredLister photoLister;                                               // {8}

    /**
     * Getter - sqlContext
     */
    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    /**
     * Setter - sqlContext
     */
    public void setSqlContext(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    /**
     * Build and returns the schema as a StructType
     * <p>
     * {s1} - используемый здесь объект schema - это супермножество схемы Spark, следовательно,
     * здесь запрашивается специализированная информация, требуемая Spark
     */
    @Override
    public StructType schema() {
        if (schema == null) {
            schema = SparkBeanUtils.getSchemaFromBean(PhotoMetadata.class);                 // {s1}
        }
        return schema.getSparkSchema();
    }

    /**
     * Цель метода - возврат данных как RDD
     * <p>
     * {b1} - необходима полная уверенность в том, что используется самая последняя схема <br>
     * {b2} - задача метода collectData() - создание списка, содержащего все метаданные о фотографиях <br>
     * {b3} - извлекаем контекст Spark из контекта SQL <br>
     * {b4} - создание RDD с использованием параллелизма <br>
     * {b5} - создание JavaRDD<Row> из таблицы с применением лямда-функции <br>
     */
    public RDD<Row> buildScan() {
        log.debug("-> buildScan");
        schema();                                                                                   // {b1}

        // I have isolated the work to a method to keep the plumbing code as
        // simple as possible
        List<PhotoMetadata> table = collectData();                                                  // {b2}

        @SuppressWarnings("resource")
        JavaSparkContext sparkContext = new JavaSparkContext(sqlContext.sparkContext());        // {b3}
        JavaRDD<Row> rowRDD = sparkContext.parallelize(table)                                   // {b4}
                .map(photo -> SparkBeanUtils.getRowFromBean(schema, photo));                // {b5}

        return rowRDD.rdd();
    }

    /**
     * Interface with the real world: the "plumbing" between Spark and
     * existing data, in our case the classes in charge of reading the
     * information from the photos
     * <p>
     * {c1} - создание списка всех файлов <br>
     * {c2} - проход в цикле по всем файлам фотографий <br>
     * {c3} - извлечение метаданных из файла <br>
     * {c4} - добавление извлеченных метеданных в список <br>
     *
     * @return The List of photos will be "mapped" and transformed into a Row
     */
    private List<PhotoMetadata> collectData() {
        List<File> photosToProcess = this.photoLister.getFiles();                                       // {c1}
        List<PhotoMetadata> list = new ArrayList<>();
        PhotoMetadata photo;

        for (File photoToProcess : photosToProcess) {                                                   // {c2}
            photo = ExifUtils.processFromFilename(photoToProcess.getAbsolutePath());   // {c3}
            list.add(photo);                                                                            // {c4}
        }
        return list;
    }

    /**
     * Setter - photoLister
     */
    public void setPhotoLister(RecursiveExtensionFilteredLister photoLister) {
        this.photoLister = photoLister;
    }

}









































