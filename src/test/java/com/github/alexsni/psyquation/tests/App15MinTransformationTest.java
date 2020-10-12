package com.github.alexsni.psyquation.tests;

import com.github.alexsni.psyquation.app.App15MinIntervals;
import com.github.alexsni.psyquation.readers.AppDataWriter;
import com.github.alexsni.psyquation.readers.CSVDataReader;
import com.github.alexsni.psyquation.readers.DataRedaer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class App15MinTransformationTest {
    private static final SparkSession spark = SparkSession.builder().appName("test_app").master("local[*]").getOrCreate();

    @Test
    public void App15MinTransformationTest() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("Sensor 0", "100", "temperature", "Room 0"));
        rows.add(RowFactory.create("Sensor 0", "200", "battery", "Room 0"));
        rows.add(RowFactory.create("Sensor 0", "300", "presence", "Room 0"));
        rows.add(RowFactory.create("Sensor 1", "100", "temperature", "Room 0"));
        rows.add(RowFactory.create("Sensor 1", "200", "battery", "Room 0"));
        rows.add(RowFactory.create("Sensor 1", "300", "presence", "Room 0"));

        StructType ds1Schema = new StructType(new StructField[]{
                new StructField("sensor_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("channel_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("channel_type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("location", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> ds1 = spark.createDataFrame(rows, ds1Schema);

        List<Row> rowsTemp = new ArrayList<>();
        rowsTemp.add(RowFactory.create("Sensor 0","100", Timestamp.valueOf(LocalDateTime.parse("2018-03-23T11:57:56")), 65.0057662358275));
        rowsTemp.add(RowFactory.create("Sensor 0","200", Timestamp.valueOf(LocalDateTime.parse("2018-03-23T11:57:56")), 100.0 ));
        rowsTemp.add(RowFactory.create("Sensor 0","300", Timestamp.valueOf(LocalDateTime.parse("2018-03-23T11:57:56")), 0.0 ));
        rowsTemp.add(RowFactory.create("Sensor 1","100", Timestamp.valueOf(LocalDateTime.parse("2018-03-23T11:57:56")), 65.0057662358275));
        rowsTemp.add(RowFactory.create("Sensor 1","200", Timestamp.valueOf(LocalDateTime.parse("2018-03-23T11:57:56")), 100.0));
        rowsTemp.add(RowFactory.create("Sensor 1","300", Timestamp.valueOf(LocalDateTime.parse("2018-03-23T11:57:56")), 0.0));

        StructType ds2Schema = new StructType(new StructField[]{
                new StructField("sensor_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("channel_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("date", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("temperature", DataTypes.DoubleType, true, Metadata.empty())

        });
        Dataset<Row> ds2 = spark.createDataFrame(rowsTemp, ds2Schema);

        Map<String, Dataset<Row>> dataMap = new HashMap<>();
        dataMap.put("ds1", ds1);
        dataMap.put("ds2", ds2);

        CSVDataReader reader1 = new CSVDataReader(null, null);
        CSVDataReader reader2 = new CSVDataReader(null, null);
        Map<String, DataRedaer> readerMap = new HashMap();
        readerMap.put("ds1", reader1);
        readerMap.put("ds2", reader2);
        AppDataWriter writer = new AppDataWriter(null, null);

        App15MinIntervals app = new App15MinIntervals(readerMap, writer, LocalDateTime.parse("2018-03-23T00:00:00").atZone(ZoneId.systemDefault()).toInstant(), LocalDateTime.parse("2018-03-24T00:00:00").atZone(ZoneId.systemDefault()).toInstant());

        Dataset<Row> result = app.transform(dataMap);
        long providedCount = result.count();

        List<Row> presence = result.where(result.col("presence").equalTo(true)).collectAsList();
        Row producedRow = presence.get(0);

        Timestamp providedStartTimeslot = producedRow.getTimestamp(0);
        String providedLocation = producedRow.getString(1);
        Double providedTempMin = producedRow.getDouble(2);
        Double providedTempMax = producedRow.getDouble(3);
        Double providedTempAvg = producedRow.getDouble(4);
        Long providedTempCnt = producedRow.getLong(5);
        Boolean providedPresence  = producedRow.getBoolean(6);
        Integer providedPresenceCnt = producedRow.getInt(7);

        assert providedCount == 97;

        assert providedStartTimeslot.equals(Timestamp.valueOf(LocalDateTime.parse("2018-03-23T11:45:00")));
        assert providedLocation.equals("Room 0");
        assert providedTempMin.equals(0.0);
        assert providedTempMax.equals(100.0);
        assert providedTempAvg.equals(55.00192207860916);
        assert providedTempCnt.equals(6L);
        assert providedPresence.equals(true);
        assert providedPresenceCnt.equals(1);


    }

}
