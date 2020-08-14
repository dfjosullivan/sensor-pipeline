package com.simulator.parsers;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SensorDataParser implements MapFunction<String, Row> {
    Timestamp col1;
    Double col2;
    Double col3;
    Double col4;
    Double col5;
    Double col6;
    Double col7;

    public static StructType getSchema() {
        StructType st = new StructType(new StructField[] {
                new StructField("date_time", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("voltage_12", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("voltage_24", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("heading", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("pitch", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("roll", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("level", DataTypes.DoubleType, false, Metadata.empty())

        }) ;
        return st;
    }

    @Override
    public Row call(String s) throws Exception {
        String[] arr = s.split(",");
        SimpleDateFormat dateFormat = new SimpleDateFormat("ddMMyyyyhhmm");
        try {
            Date parsedDate = dateFormat.parse(arr[0]);
            col1 = new java.sql.Timestamp(parsedDate.getTime());
            col2 = new Double(arr[1]);
            col3 = new Double(arr[2]);
            col4 = new Double(arr[3]);
            col5 = new Double(arr[4]);
            col6 = new Double(arr[5]);
            col7 = new Double(arr[6]);
        } catch (Exception e) {

        }
        return RowFactory.create(col1, col2, col3, col4, col5, col6, col7);
    }
}
