package com.simulator.parsers;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AtmosphericDataParser implements MapFunction<String, Row> {
    Timestamp col1;
    Double col2;
    int col3;
    BigDecimal col4;

    public static StructType getSchema() {
        StructType st = new StructType(new StructField[]{
                new StructField("date", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("temperature", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("humidity", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("pressure", DataTypes.createDecimalType(38, 15), false, Metadata.empty())
        });
        return st;
    }

    @Override
    public Row call(String s) throws Exception {
        String[] arr = s.split(",");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
        try {
            Date parsedDate = dateFormat.parse(arr[0]);
            col1 = new java.sql.Timestamp(parsedDate.getTime());
            col2 = new Double(arr[1]);
            col3 = Integer.parseInt(arr[2]);
            col4 = new BigDecimal(arr[3]).setScale(15, BigDecimal.ROUND_UP);
        } catch (Exception e) {
            System.out.println("Problem is on line: " + s);
            e.printStackTrace();
        }
        return RowFactory.create(col1, col2, col3, col4);
    }
}
