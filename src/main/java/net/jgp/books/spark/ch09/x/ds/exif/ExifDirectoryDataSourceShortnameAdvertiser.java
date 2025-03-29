package net.jgp.books.spark.ch09.x.ds.exif;

import org.apache.spark.sql.sources.DataSourceRegister;     // {1}

/**
 * Заявочный класс - отвечает за предъявление (заявление) короткого имени, по
 * которому можно вызывать источник данных
 * <p>
 * {1} - вызываем интерфейс DataSourceRegister, чтобы выполнить его реализацию <br>
 * {2} - наследуемся от класса ExifDirectoryDataSource, где находится код источника <br>
 * {3} - реализация метода shortName(), чтобы возвращать короткое имя, которое необходимо
 * использовать для этого источника <br>
 */
public class ExifDirectoryDataSourceShortnameAdvertiser
        extends ExifDirectoryDataSource implements DataSourceRegister {     // {2}

    @Override
    public String shortName() {             // {3}
        return "exif";
    }

}
