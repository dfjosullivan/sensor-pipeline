package test.java.com.outliers;

import main.java.com.outliers.parsers.ElseParser;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ElseParserTest {

    @Test
    void getElseSchema() {
        StructType actual = ElseParser.getElseSchema();
        StructType expected = new StructType(new StructField[]{
                new StructField("date_time", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("voltage_12", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("voltage_24", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("wind_direction", DataTypes.createDecimalType(38, 15), false, Metadata.empty()),
                new StructField("wind_speed", DataTypes.createDecimalType(38, 15), false, Metadata.empty()),
                new StructField("wind_gust", DataTypes.createDecimalType(38, 15), false, Metadata.empty()),
                new StructField("level", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("water_direction", DataTypes.createDecimalType(38, 15), false, Metadata.empty()),
                new StructField("water_speed_cm_s", DataTypes.createDecimalType(38, 15), false, Metadata.empty()),
                new StructField("pressure", DataTypes.createDecimalType(38, 15), false, Metadata.empty()),
                new StructField("temperature", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("humidity", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("heading", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("pitch", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("roll", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("water_speed_kn", DataTypes.DoubleType, false, Metadata.empty()),
        });
        assertEquals(actual, expected);
    }
}