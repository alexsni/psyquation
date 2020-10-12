package com.github.alexsni.psyquation.readers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Map;

public class AppDataWriter {
    private String outputPath;
    private Map<String, String> options;

    public AppDataWriter(String outputPath, Map<String, String> options) {
        this.outputPath = outputPath;
        this.options = options;
    }

    public void writeJson(Dataset<Row> df) {
        df.write().mode(SaveMode.Overwrite).options(options).json(outputPath);
    }
}
