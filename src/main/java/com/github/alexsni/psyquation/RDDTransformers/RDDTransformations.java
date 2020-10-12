package com.github.alexsni.psyquation.RDDTransformers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.LinkedList;
import java.util.List;

public abstract class RDDTransformations<T, O> {
    private String appName;
    private String master;
    private String validateOutputSpecs;
    private JavaSparkContext spark;
    private String sourceLocation;
    private String outputLocation;
    Class<T> inputDataClass;
    Class<O> outputDataClass;

    public RDDTransformations(String appName,
                              String master,
                              String validateOutputSpecs,
                              String sourceLocation,
                              String outputLocation,
                              Class<T> inputDataClass,
                              Class<O> outputDataClass) {
        this.appName = appName;
        this.master = master;
        this.validateOutputSpecs = validateOutputSpecs;
        this.spark = getSpark();
        this.sourceLocation = sourceLocation;
        this.outputLocation = outputLocation;
        this.inputDataClass = inputDataClass;
        this.outputDataClass = outputDataClass;
    }

    private JavaSparkContext getSpark() {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master).set("spark.hadoop.validateOutputSpecs", validateOutputSpecs);
        return new JavaSparkContext(conf);
    }

    private JavaRDD<T> convertToObject(JavaRDD<String> input, Class<T> clazz) {
        return input.mapPartitions(row -> {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
            objectMapper.registerModule(new JavaTimeModule());
            List<T> ret = new LinkedList<>();
            row.forEachRemaining(e -> {
                try {
                    ret.add(objectMapper.readValue(e, clazz));
                } catch (JsonProcessingException ex) {
                    ret.add(null);
                    ex.printStackTrace();
                }
            });
            return ret.iterator();
        });
    }

    private JavaRDD<String> convertToJsonString(JavaRDD<O> out) {
        return out.map(row ->  {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(row);
        });
    }

    public abstract JavaRDD<O> transform(JavaRDD<T> inputDf);

    private JavaRDD<String> readFile() {
        return spark.textFile(sourceLocation);
    }

    private void writeOutput(JavaRDD<String> input) {
        input.saveAsTextFile(outputLocation);
    }

    public void runItAll() {
        writeOutput(convertToJsonString(transform(convertToObject(readFile(), inputDataClass))));
    }

}

