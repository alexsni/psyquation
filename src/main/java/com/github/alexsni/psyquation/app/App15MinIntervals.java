package com.github.alexsni.psyquation.app;

import com.github.alexsni.psyquation.readers.AppDataWriter;
import com.github.alexsni.psyquation.readers.CSVDataReader;
import com.github.alexsni.psyquation.readers.DataRedaer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public class App15MinIntervals extends AbstractDFApp {
    private static final StructType ds1Schema = new StructType(new StructField[]{
            new StructField("sensor_id", DataTypes.StringType, true, Metadata.empty()),
            new StructField("channel_id", DataTypes.StringType, true, Metadata.empty()),
            new StructField("channel_type", DataTypes.StringType, true, Metadata.empty()),
            new StructField("location", DataTypes.StringType, true, Metadata.empty())
    });
    private static final StructType ds2Schema = new StructType(new StructField[]{
            new StructField("sensor_id", DataTypes.StringType, true, Metadata.empty()),
            new StructField("channel_id", DataTypes.StringType, true, Metadata.empty()),
            new StructField("date", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("temperature", DataTypes.DoubleType, true, Metadata.empty())

    });
    Instant start;
    Instant end;

    public App15MinIntervals(Map<String, DataRedaer> dataRedaerMapeaders, AppDataWriter appDataWriter, Instant start, Instant end) {
        super(dataRedaerMapeaders, appDataWriter);
        this.start = start;
        this.end = end;
    }

    @Override
    public Dataset<Row> transform(Map<String, Dataset<Row>> inputDataFrame) {
        Dataset<Row> sensorRefData = inputDataFrame.get("ds1");

        Dataset<Row> sensorEvents = inputDataFrame.get("ds2")
                .filter(functions.col("date").lt(functions.lit(end)))
                .filter(functions.col("date").gt(functions.lit(start)));

        Dataset<Row> joined = sensorRefData.join(sensorEvents, "sensor_id").where(sensorRefData.col("channel_id").equalTo(sensorEvents.col("channel_id")));
        Dataset<Row> allTimeSlots = fillTImeslots(start.getEpochSecond(), end.getEpochSecond()); // getSpark().range((minp / step) * step, ((maxp / step) + 1) * step, step).select(functions.col("id").cast("timestamp").alias("periodstart"));
        Dataset<Row> slised = joined.groupBy(functions.window(functions.col("date"), "15 minutes").alias("periodstart"), joined.col("location"))
                .agg(   functions.when(functions.min(functions.col("temperature")).isNaN(), 0).otherwise(functions.min(functions.col("temperature"))).alias("tempMin"),
                        functions.when(functions.max(functions.col("temperature")).isNull(), 0).otherwise(functions.max(functions.col("temperature"))).alias("tempMax"),
                        functions.when(functions.mean(functions.col("temperature")).isNull(), 0).otherwise(functions.mean(functions.col("temperature"))).alias("tempAvg"),
                        functions.when(functions.count(functions.col("temperature")).isNull(), 0).otherwise(functions.count(functions.col("temperature"))).alias("tempCnt")
                ).orderBy(functions.col("window"))
                .withColumnRenamed("periodstart", "timeSlotStart");
        Dataset<Row> result = allTimeSlots.join(slised, allTimeSlots.col("periodstart").equalTo(slised.col("timeSlotStart.start")).alias("window"), "leftouter")
                .select(
                        functions.col("periodstart").alias("timeSlotStart"),
                        functions.when(functions.col("location").isNull(), "").otherwise(functions.col("location")).alias("location"),
                        functions.when(functions.col("tempMin").isNull(), 0).otherwise(functions.col("tempMin")).alias("tempMin"),
                        functions.when(functions.col("tempMax").isNull(), 0).otherwise(functions.col("tempMax")).alias("tempMax"),
                        functions.when(functions.col("tempAvg").isNull(), 0).otherwise(functions.col("tempAvg")).alias("tempAvg"),
                        functions.when(functions.col("tempCnt").isNull(), 0).otherwise(functions.col("tempCnt")).alias("tempCnt"),
                        functions.when(functions.col("tempCnt").isNull(), false).otherwise(true).alias("presence"),
                        functions.when(functions.col("tempCnt").isNull(), 0).otherwise(1).alias("presenceCnt")
                );
        return result.orderBy(result.col("timeSlotStart")).coalesce(1);
    }

    private Dataset<Row> fillTImeslots(Long minp, Long maxp) {
        long step = 15 * 60;
        return getSpark().range((minp / step) * step, ((maxp / step) + 1) * step, step).select(functions.col("id").cast("timestamp").alias("periodstart"));
    }


    public static final Map<String, DataRedaer> setupReaders() {
        DataRedaer ds1reader = new CSVDataReader(
                App15MinIntervals.class.getClassLoader().getResource("ds1.csv").toString(),
                ds1Schema);
        DataRedaer ds2reader = new CSVDataReader(
                App15MinIntervals.class.getClassLoader().getResource("ds2.csv").toString(),
                ds2Schema
        );
        Map<String, DataRedaer> readerMap = new HashMap<>();
        readerMap.put("ds1", ds1reader);
        readerMap.put("ds2", ds2reader);
        return readerMap;
    }


    private static final Map<String, String> setOutputOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("dropFieldIfAllNull", "true");
        options.put("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss");
        return options;
    }

    public static void main(String[] args) {
        Instant start = LocalDateTime.parse(args[0]).atZone(ZoneId.systemDefault()).toInstant();
        Instant end = LocalDateTime.parse(args[1]).atZone(ZoneId.systemDefault()).toInstant();
        String outputLocation = args[2];

        System.out.println("Starting program with args: start window [" + start + "]" + "end window [" + end + "]");

        AppDataWriter jsonWriter = new AppDataWriter(outputLocation, setOutputOptions());
        App15MinIntervals self = new App15MinIntervals(setupReaders(), jsonWriter, start, end);
        self.startProcess();
    }
}
