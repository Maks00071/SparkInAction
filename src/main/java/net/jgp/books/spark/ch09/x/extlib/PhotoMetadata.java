package net.jgp.books.spark.ch09.x.extlib;

import java.io.Serializable;
import java.nio.file.attribute.FileTime;
import java.sql.Timestamp;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch09.x.utils.SparkColumn;                    // {1}


/**
 * A good old JavaBean containing the EXIF properties as well as the
 * SparkColumn annotation
 * <p>
 * {1} - аннотация @SparkColumn - совет по преобразованию компоненты JavaBean в фрейм данных <br>
 * {2} - свойства, еоторые будут отображаться в столбцы Spark <br>
 * {3} - используя аннотацию, можно задать имя столбца, в противном случае будет использовано имя свойства в
 * стиле camel с использованием верхнего регистра <br>
 * {4} - явное определение недопустимости нулевого значения, т.к. имя файла обязательно на уровне API
 * источника данных <br>
 * {5} - аннотация может явно и принудительно определять тип, но следует внимательно относиться
 * к этому преобразованию, т.к. Spark не будет знать, как преобразовать один тип в другой <br>
 */
public class PhotoMetadata implements Serializable {

    private static transient Logger log = LoggerFactory.getLogger(PhotoMetadata.class);
    private static final long serialVersionUID = -2589804417011601051L;
    // {2}
    private Timestamp dateTaken;
    private String directory;
    private String extension;
    private Timestamp fileCreationDate;
    private Timestamp fileLastAccessDate;
    private Timestamp fileLastModifiedDate;
    private String filename;
    private Float geoX;
    private Float geoY;
    private Float geoZ;
    private int height;
    private String mimeType;
    private String name;
    private long size;
    private int width;
    // {2}

    /**
     * @return the dateTaken
     */
    @SparkColumn(name = "Date")                                             // {3}
    public Timestamp getDateTaken() {
        return dateTaken;
    }

    /**
     * @return the directory
     */
    public String getDirectory() {
        return directory;
    }

    /**
     * @return the extension
     */
    public String getExtension() {
        return extension;
    }

    /**
     * @return the fileCreationDate
     */
    public Timestamp getFileCreationDate() {
        return fileCreationDate;
    }

    /**
     * @return the fileLastAccessDate
     */
    public Timestamp getFileLastAccessDate() {
        return fileLastAccessDate;
    }

    /**
     * @return the fileLastModifiedDate
     */
    public Timestamp getFileLastModifiedDate() {
        return fileLastModifiedDate;
    }

    /**
     * @return the filename
     */
    @SparkColumn(nullable = false)                                           // {4}
    public String getFilename() {
        return filename;
    }

    /**
     * @return the geoX
     */
    @SparkColumn(type = "float")                                            // {5}
    public Float getGeoX() {
        return geoX;
    }

    /**
     * @return the geoY
     */
    public Float getGeoY() {
        return geoY;
    }

    /**
     * @return the geoZ
     */
    public Float getGeoZ() {
        return geoZ;
    }

    /**
     * @return the height
     */
    public int getHeight() {
        return height;
    }

    /**
     * @return the mimeType
     */
    public String getMimeType() {
        return mimeType;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the size
     */
    public long getSize() {
        return size;
    }

    /**
     * @return the width
     */
    public int getWidth() {
        return width;
    }

    /**
     * @param date
     */
    public void setDateTaken(Date date) {
        if (date == null) {
            log.warn("Attempt to set a null date.");
            return;
        }
        setDateTaken(new Timestamp((date.getTime())));
    }

    /**
     * @param dateTaken the dateTaken to set
     */
    public void setDateTaken(Timestamp dateTaken) {
        this.dateTaken = dateTaken;
    }

    /**
     * @param directory the directory to set
     */
    public void setDirectory(String directory) {
        this.directory = directory;
    }

    /**
     * @param extension the extension to set
     */
    public void setExtension(String extension) {
        this.extension = extension;
    }

    public void setFileCreationDate(FileTime creationTime) {
        setFileCreationDate(new Timestamp(creationTime.toMillis()));
    }

    /**
     * @param fileCreationDate the fileCreationDate to set
     */
    public void setFileCreationDate(Timestamp fileCreationDate) {
        this.fileCreationDate = fileCreationDate;
    }

    public void setFileLastAccessDate(FileTime lastAccessDate) {
        setFileLastAccessDate(new Timestamp(lastAccessDate.toMillis()));
    }

    /**
     * @param fileLastAccessDate the fileLastAccessDate to set
     */
    public void setFileLastAccessDate(Timestamp fileLastAccessDate) {
        this.fileLastAccessDate = fileLastAccessDate;
    }

    public void setFileLastModifiedDate(FileTime fileLastModifiedDate) {
        setFileLastModifiedDate(new Timestamp(fileLastModifiedDate.toMillis()));
    }

    /**
     * @param fileLastModifiedDate the fileLastModifiedDate to set
     */
    public void setFileLastModifiedDate(Timestamp fileLastModifiedDate) {
        this.fileLastModifiedDate = fileLastModifiedDate;
    }

    /**
     * @param filename the fileName to set
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

    /**
     * @param geoX the geoX to set
     */
    public void setGeoX(Float geoX) {
        this.geoX = geoX;
    }

    /**
     * @param geoY the geoY to set
     */
    public void setGeoY(Float geoY) {
        this.geoY = geoY;
    }

    /**
     * @param geoZ the geoZ to set
     */
    public void setGeoZ(Float geoZ) {
        this.geoZ = geoZ;
    }

    /**
     * @param height the height to set
     */
    public void setHeight(int height) {
        this.height = height;
    }

    /**
     * @param mimeType the mimeTyoe to set
     */
    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @param size the size to set
     */
    public void setSize(long size) {
        this.size = size;
    }

    /**
     * @param width the width to set
     */
    public void setWidth(int width) {
        this.width = width;
    }
}
























































