package com.simulator.parsers;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SensorDataParserTest {

    @Test
    void getSchema() {
        StructType actual = SensorDataParser.getSchema();
        StructType expected = new StructType(new StructField[] {
                new StructField("date_time", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("voltage_12", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("voltage_24", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("heading", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("pitch", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("roll", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("level", DataTypes.DoubleType, false, Metadata.empty())

        }) ;
        assertEquals(actual, expected);
    }
}