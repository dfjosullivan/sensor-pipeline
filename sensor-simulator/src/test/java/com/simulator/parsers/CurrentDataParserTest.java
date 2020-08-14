package com.simulator.parsers;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CurrentDataParserTest {

    @Test
    void getSchema() {
        StructType actual = CurrentDataParser.getSchema();
        StructType expected = new StructType(new StructField[]{
                new StructField("date", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("water_direction", DataTypes.createDecimalType(38, 15), false, Metadata.empty()),
                new StructField("water_speed_cm_s", DataTypes.createDecimalType(38, 15), false, Metadata.empty())
        });
        assertEquals(actual, expected);
    }
}