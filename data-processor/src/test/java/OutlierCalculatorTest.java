import com.outliers.OutlierCalculator;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class OutlierCalculatorTest {

    @Test
    void getDatasetWithOutliers() {
        SparkSession ss = SparkSession.builder()
                .master("local")
                .appName("Unit tests")
                .getOrCreate();
        ArrayList<Integer> data = new ArrayList<>(Arrays.asList(5,5,6,4,7,4,15,1));
        int col1 = 0;
        Dataset<Row> actual = OutlierCalculator.getDatasetWithOutliers(ss.createDataset(data, Encoders.INT()).as(Encoders.bean(Row.class)), "value");
        Dataset<Row> expected = ss.createDataset(data, Encoders.INT()).as(Encoders.bean(Row.class));
        double[] quantile = expected.stat().approxQuantile("value", new double[]{0.25, 0.75}, 0d);
        double q3 = quantile[1];
        double q1 = quantile[0];
        double iqr = q3-q1;
        expected = expected.withColumn("value_outlier",
                functions.when(expected.col("value").geq(q3 + 1.5 * iqr), true)
                        .when(expected.col("value").leq(q1 - 1.5 * iqr), true).otherwise(false));
        assertArrayEquals(expected.collectAsList().toArray(), actual.collectAsList().toArray());
    }
}