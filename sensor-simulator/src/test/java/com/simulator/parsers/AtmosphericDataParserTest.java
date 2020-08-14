package com.simulator.parsers;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AtmosphericDataParserTest {

    @Test
    void getSchema() {
        StructType actual = AtmosphericDataParser.getSchema();
        StructType expected = new StructType(new StructField[]{
                new StructField("date", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("temperature", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("humidity", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("pressure", DataTypes.createDecimalType(38, 15), false, Metadata.empty())
        });

        assertEquals(actual, expected);
    }
}