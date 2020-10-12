package com.github.alexsni.psyquation.readers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class CSVDataReader implements DataRedaer {
    String inputPath;
    StructType schema;

    public CSVDataReader(String inputPath) {
        this.inputPath = inputPath;
    }

    public CSVDataReader(String inputPath, StructType schema) {
        this.inputPath = inputPath;
        this.schema = schema;
    }

    @Override
    public Dataset<Row> readFile(SparkSession sparkSession) {
        if (null != schema)
            return sparkSession.read().schema(schema).csv(inputPath);
        else
            return sparkSession.read().csv(inputPath);
    }

}
