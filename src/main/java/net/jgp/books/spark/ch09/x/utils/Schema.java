package net.jgp.books.spark.ch09.x.utils;

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Stores the Spark schema as well as extra information we cannot add to the
 * (Spark) schema (Простой контейнер для хоанения схемы Spark и дополнительнеой информации)
 */
public class Schema implements Serializable {

    private static final long serialVersionUID = 2376325490075130182L;

    private StructType structSchema;
    private Map<String, SchemaColumn> columns;

    public Schema() {
        columns = new HashMap<>();
    }

    /**
     * @return the structSchema
     */
    public StructType getSparkSchema() {
        return structSchema;
    }

    /**
     * @param structSchema
     *          the struckSchema to set
     */
    public void setSparkSchema(StructType structSchema) {
        this.structSchema = structSchema;
    }

    /**
     * @param column
     *          the column add (key, value) to the columns
     */
    public void add(SchemaColumn column) {
        this.columns.put(column.getColumnName(), column);
    }

    /**
     * @param columnName
     *          the columnName
     * @return the methodName
     */
    public String getMethodName(String columnName) {
        return this.columns.get(columnName).getMethodName();
    }
}




































