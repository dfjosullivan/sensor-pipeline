package com.outliers;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class OutlierCalculator {


    public static Dataset<Row> getDatasetWithOutliers(Dataset<Row> originalDataset, String... columns) {

        double[][] quantiles = originalDataset.stat().approxQuantile(columns, new double[]{0.25, 0.75}, 0d);
        Dataset<Row> speedDataWithOutliers = originalDataset;
        try {
            for (String s : columns) {
                int index = ArrayUtils.indexOf(columns, s);
                speedDataWithOutliers = speedDataWithOutliers.withColumn(s + "_outlier",
                        functions.when(originalDataset.col(s).geq((quantiles[index][1] + 1.5 * (quantiles[index][1] - quantiles[index][0])))
                                , true)
                                .when(originalDataset.col(s).leq((quantiles[index][0] - 1.5 * (quantiles[index][1] - quantiles[index][0]))), true).otherwise(false));
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Dataset has no values to calculate outliers");
            e.printStackTrace();
        }

        return speedDataWithOutliers;
    }


}
