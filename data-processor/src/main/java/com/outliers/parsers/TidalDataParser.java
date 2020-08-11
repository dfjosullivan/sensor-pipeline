package main.java.com.outliers.parsers;

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

public class TidalDataParser implements MapFunction<String, Row> {
    Timestamp col1;
    Double col2;
    Double col3;

    public TidalDataParser() {
    }

    public static StructType getSchema() {
        StructType st = new StructType(new StructField[]{
                new StructField("date", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("predicted", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("actual", DataTypes.DoubleType, false, Metadata.empty()),
        });
        return st;
    }

    @Override
    public Row call(String s) throws Exception {
        String[] arr = s.split(",");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
        Date parsedDate = dateFormat.parse(arr[0]);
        col1 = new java.sql.Timestamp(parsedDate.getTime());
        col2 = new Double(arr[1]);
        col3 = new Double(arr[2]);
        return RowFactory.create(col1, col2, col3);
    }
}
