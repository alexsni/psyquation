package com.github.alexsni.psyquation.app;

import com.github.alexsni.psyquation.readers.AppDataWriter;
import com.github.alexsni.psyquation.readers.DataRedaer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractDFApp {
    private SparkSession spark;
    private Map<String, DataRedaer> dataRedaerMapeaders;
    private AppDataWriter appDataWriter;

    public AbstractDFApp(Map<String, DataRedaer> dataRedaerMapeaders, AppDataWriter appDataWriter) {
        this.spark = builsSparkSession();
        this.dataRedaerMapeaders = dataRedaerMapeaders;
        this.appDataWriter = appDataWriter;
    }

    public SparkSession getSpark() {
        return spark;
    }

    private SparkSession builsSparkSession() {
        return SparkSession.builder().appName("test_app").master("local[*]").getOrCreate();
    }

    private Map<String, Dataset<Row>> readData() {
        return dataRedaerMapeaders.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> e.getValue().readFile(spark)));
    }

    public abstract Dataset<Row> transform(Map<String, Dataset<Row>> inputDataFrame);

    public void startProcess() {
        appDataWriter.writeJson(transform(readData()));
    }

}
