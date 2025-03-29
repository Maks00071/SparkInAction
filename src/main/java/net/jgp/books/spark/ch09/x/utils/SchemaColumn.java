package net.jgp.books.spark.ch09.x.utils;

import java.io.Serializable;

/**
 * Объект, используемый для хранения дополнительных метаданных о столбце
 */
public class SchemaColumn implements Serializable {

    private static final long serialVersionUID = 9113201899451270469L;
    private String methodName;
    private String columnName;

    /**
     * @return the methodName
     */
    public String getMethodName() {
        return methodName;
    }

    /**
     * @param methodName
     *          the methodName to set
     */
    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    /**
     * @return the columnName
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * @param columnName
     *          the columnName to set
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
