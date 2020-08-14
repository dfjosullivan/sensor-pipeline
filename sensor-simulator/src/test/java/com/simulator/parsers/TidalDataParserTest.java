package com.simulator.parsers;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.sql.Struct;

import static org.junit.jupiter.api.Assertions.*;

class TidalDataParserTest {

    @Test
    void getSchema() {
        StructType actual = TidalDataParser.getSchema();
        StructType expected = new StructType(new StructField[] {
                new StructField("date", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("predicted", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("actual", DataTypes.DoubleType, false, Metadata.empty()),
        }) ;
        assertEquals(actual, expected);
    }
}