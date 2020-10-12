package com.github.alexsni.psyquation.readers;

import org.apache.spark.internal.config.R;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public interface DataRedaer {
    Dataset<Row> readFile(SparkSession sparkSession);
}
