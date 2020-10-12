package com.github.alexsni.psyquation.tests;

import com.github.alexsni.psyquation.readers.CSVDataReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class CSVDataReaderTest {
    private static final SparkSession spark = SparkSession.builder().appName("test_app").master("local[*]").getOrCreate();;

    @Test
    public void testCSVReaderWithoutSchema() {
        CSVDataReader reader = new CSVDataReader(CSVDataReaderTest.class.getClassLoader().getResource("test_ds1.csv").toString());
        Dataset<Row> rowDataset = reader.readFile(spark);

        assert rowDataset.count() == 25;
    }

    private boolean compareFields(StructField[] actualFields, StructField[] expectedFields) {
        if (actualFields.length != expectedFields.length) return false;
        for (int i=0; i < actualFields.length; i++) {
            if (! actualFields[i].equals(expectedFields[i]))
                return false;
        }
        return true;
    }

    @Test
    public void testCSVReaderWithSchema() {
        StructType ds1Schema = new StructType(new StructField[]{
                new StructField("sensor_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("channel_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("channel_type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("location", DataTypes.StringType, true, Metadata.empty())
        });
        CSVDataReader reader = new CSVDataReader(
                CSVDataReaderTest.class.getClassLoader().getResource("test_ds1.csv").toString(),
                ds1Schema);
        Dataset<Row> rowDataset = reader.readFile(spark);

        assert compareFields(rowDataset.schema().fields(), ds1Schema.fields());
    }
}
