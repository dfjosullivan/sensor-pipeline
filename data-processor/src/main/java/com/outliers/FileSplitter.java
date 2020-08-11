package com.outliers;

import main.java.com.outliers.parsers.ElseParser;
import main.java.com.outliers.parsers.TidalDataParser;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class FileSplitter {
    final FTPClient ftpClient;
    final String username;
    final String password;
    final String server;
    final boolean localMode;
    final SparkSession ss;


    public FileSplitter(String username, String password, String server, boolean localMode, SparkSession ss) {
        this.ftpClient = new FTPClient();
        this.username = username;
        this.password = password;
        this.server = server;
        this.localMode = localMode;
        this.ss = ss;
    }

    public void splitFiles() {
        Path tidalTempFile = null;
        Path elseTempFile = null;
        try {
            tidalTempFile = Files.createTempFile(null, UUID.randomUUID().toString());
            elseTempFile = Files.createTempFile(null, UUID.randomUUID().toString());
            new File(tidalTempFile.toString()).deleteOnExit();
            new File(elseTempFile.toString()).deleteOnExit();

        } catch (
                IOException e) {
            e.printStackTrace();
        }
        OutputStream tidalOutputStream = null;
        OutputStream elseOutputStream = null;
        try {
            this.ftpClient.connect(server);
            this.ftpClient.login(username, password);
            if (localMode) {
                this.ftpClient.enterLocalPassiveMode();
            }
            tidalOutputStream = new FileOutputStream(tidalTempFile.toString());
            elseOutputStream = new FileOutputStream(elseTempFile.toString());
            this.ftpClient.retrieveFile("everything-else-2019.csv",  elseOutputStream);
            this.ftpClient.retrieveFile("tidal-data-2019.csv", tidalOutputStream);

            tidalOutputStream.close();
            elseOutputStream.close();
            this.ftpClient.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Dataset<Row> tidalDataset = ss.read().schema(TidalDataParser.getSchema()).csv(tidalTempFile.toString());
        Dataset<Row> elseDataset = ss.read().schema(ElseParser.getElseSchema()).csv(elseTempFile.toString());
        Dataset<Row> tidalToWrite = OutlierCalculator.getDatasetWithOutliers(tidalDataset, "actual", "predicted")
                .withColumn("difference", tidalDataset.col("actual").minus(tidalDataset.col("predicted")));
        Dataset<Row> windDatasetToWrite = OutlierCalculator.getDatasetWithOutliers(
                elseDataset.select("date_time", "wind_speed", "wind_direction", "wind_gust"),
                "wind_speed", "wind_direction");
        Dataset<Row> currentDatasetToWrite = OutlierCalculator.getDatasetWithOutliers(
                elseDataset.select("date_time", "water_speed_cm_s", "water_direction"),
                "water_speed_cm_s", "water_direction");
        Dataset<Row> atmosphereDatasetToWrite = elseDataset.select("date_time", "pressure", "temperature");
        Dataset<Row> sensorDatasetToWrite = elseDataset.select("date_time", "voltage_12", "voltage_24", "heading", "pitch", "roll");

        String tidalPath = System.getProperty("java.io.tmpdir") + "tidalDataset" + username;
        String windPath = System.getProperty("java.io.tmpdir") + "windDataset" + username;
        String currentPath = System.getProperty("java.io.tmpdir") + "currentDataset" + username;
        String atmPath = System.getProperty("java.io.tmpdir") + "atmDataset" + username;
        String sensorPath = System.getProperty("java.io.tmpdir") + "sensorDataset" + username;

        tidalToWrite.coalesce(1).write().mode(SaveMode.Overwrite).csv(tidalPath);
        windDatasetToWrite.coalesce(1).write().mode(SaveMode.Overwrite).csv(windPath);
        currentDatasetToWrite.coalesce(1).write().mode(SaveMode.Overwrite).csv(currentPath);
        atmosphereDatasetToWrite.coalesce(1).write().mode(SaveMode.Overwrite).csv(atmPath);
        sensorDatasetToWrite.coalesce(1).write().mode(SaveMode.Overwrite).csv(sensorPath);

    }

    public String getFilePath(String dataset) {
        return getFilePath(dataset, this.username);
    }

    public static String getFilePath(String dataset, String username) {
        String filePath = System.getProperty("java.io.tmpdir") + dataset + username;
        File file = new File(filePath);
        String resultFile = null;
        for (File listFile : file.listFiles()) {
            listFile.deleteOnExit();
            if (listFile.getName().endsWith(".csv")) {
                resultFile = listFile.getAbsolutePath();
            }
        }
        return resultFile;
    }

}
