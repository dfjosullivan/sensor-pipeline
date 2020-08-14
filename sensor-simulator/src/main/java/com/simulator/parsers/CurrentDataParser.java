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

public class CurrentDataParser implements MapFunction<String, Row> {
    Timestamp col1;
    BigDecimal col2;
    BigDecimal col3;

    public static StructType getSchema() {
        StructType st = new StructType(new StructField[]{
                new StructField("date", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("water_direction", DataTypes.createDecimalType(38, 15), false, Metadata.empty()),
                new StructField("water_speed_cm_s", DataTypes.createDecimalType(38, 15), false, Metadata.empty())
        });
        return st;
    }

    @Override
    public Row call(String s) throws Exception {
        String[] arr = s.split(",");
        if (arr.length == 0) {

        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("ddMMyyyyhhmm");
        try {
            Date parsedDate = dateFormat.parse(arr[0]);
            col1 = new java.sql.Timestamp(parsedDate.getTime());
            col2 = new BigDecimal(arr[1]).setScale(15, BigDecimal.ROUND_UP);
            col3 = new BigDecimal(arr[2]).setScale(15, BigDecimal.ROUND_UP);
        } catch (Exception e) {
            System.out.println("Problem is on line: " + s);
            e.printStackTrace();
        }
        return RowFactory.create(col1, col2, col3);
    }
}
