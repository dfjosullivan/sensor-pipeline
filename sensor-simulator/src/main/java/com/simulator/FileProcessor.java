package com.simulator;

import com.microsoft.azure.storage.StorageException;
import com.simulator.parsers.*;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.apache.spark.sql.functions.current_timestamp;

public class FileProcessor {

    AzureConnector connector;
    String containerName;

    public FileProcessor(AzureConnector connector, String containerName) {
        this.connector = connector;
        this.containerName = containerName;
    }

    public void processElseDataset(SparkSession ss, boolean timenow){

        Path sensorTempFile = null;
        Path windTempFile = null;
        Path currentTempFile = null;
        Path atmosphericTempFile = null;
        try {
            sensorTempFile = Files.createTempFile(null, UUID.randomUUID().toString());
            windTempFile = Files.createTempFile(null, UUID.randomUUID().toString());;
            currentTempFile = Files.createTempFile(null, UUID.randomUUID().toString());
            atmosphericTempFile = Files.createTempFile(null, UUID.randomUUID().toString());
            new File(sensorTempFile.toString()).deleteOnExit();
            new File(windTempFile.toString()).deleteOnExit();
            new File(currentTempFile.toString()).deleteOnExit();
            new File(atmosphericTempFile.toString()).deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            this.connector.getFile(containerName, "sensor-data-2019.csv").downloadToFile(sensorTempFile.toString());
            this.connector.getFile(containerName, "wind-data-2019.csv").downloadToFile(windTempFile.toString());
            this.connector.getFile(containerName, "atmospheric-data-2019.csv").downloadToFile(atmosphericTempFile.toString());
            this.connector.getFile(containerName, "current-data-2019.csv").downloadToFile(currentTempFile.toString());
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


        Dataset<String> windFile = ss.read().textFile(windTempFile.toString());
        final WindDataParser windParser = new WindDataParser();
        Dataset<Row> windDataset = windFile.filter((FilterFunction<String>) x -> x.split(",").length != 0).map(windParser, RowEncoder.apply(windParser.getSchema()));

        Dataset<String> atmFile = ss.read().textFile(atmosphericTempFile.toString());
        final AtmosphericDataParser atmParser = new AtmosphericDataParser();
        Dataset<Row> atmDataset = atmFile.filter((FilterFunction<String>) x -> x.split(",").length != 0).map(atmParser, RowEncoder.apply(atmParser.getSchema()));

        Dataset<String> currentFile = ss.read().textFile(currentTempFile.toString());
        final CurrentDataParser currentDataParser = new CurrentDataParser();
        Dataset<Row> currentDataset = currentFile.filter((FilterFunction<String>) x -> x.split(",").length != 0).map(currentDataParser, RowEncoder.apply(currentDataParser.getSchema()));

        Dataset<String> sensorFile = ss.read().textFile(sensorTempFile.toString());
        final SensorDataParser sensorDataParser = new SensorDataParser();
        Dataset<Row> sensorDataset = sensorFile.filter((FilterFunction<String>) x -> x.split(",").length != 0).map(sensorDataParser, RowEncoder.apply(sensorDataParser.getSchema()));

        Dataset<Row> everythingElse = sensorDataset.join(windDataset, sensorDataset.col("date_time").equalTo(windDataset.col("date")), "full_outer")
                .join(currentDataset, sensorDataset.col("date_time").equalTo(currentDataset.col("date")), "full_outer")
                .join(atmDataset, sensorDataset.col("date_time").equalTo(atmDataset.col("date")), "full_outer");

        Dataset<Row> finalDataset = everythingElse.select("date_time", "voltage_12", "voltage_24", "wind_direction", "wind_speed", "wind_gust", "level", "water_direction", "water_speed_cm_s", "pressure", "temperature", "humidity",
                "heading", "pitch", "roll").withColumn("water_speed_kn", currentDataset.col("water_speed_cm_s").divide(new Double("51.444")));
        if (timenow) {
            finalDataset = finalDataset.withColumn("date_time", current_timestamp());
        }
        String filePath = System.getProperty("java.io.tmpdir") + "combinedData";
        finalDataset.coalesce(1).write().mode(SaveMode.Overwrite).option("timestampFormat", "yyyy-MM-dd'T'hh:mm:ss").csv(filePath);

    }

    public void processTidalDataset(SparkSession ss, boolean timenow){
        Path tidalTempFile = null;
        try {
            tidalTempFile = Files.createTempFile(null, UUID.randomUUID().toString());
            new File(tidalTempFile.toString()).deleteOnExit();
            this.connector.getFile(containerName, "tidal-data-2019.csv").downloadToFile(tidalTempFile.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        Dataset<String> tidalFile = ss.read().textFile(tidalTempFile.toString());
        final TidalDataParser tidalDataParser = new TidalDataParser();
        Dataset<Row> tidalDataset = tidalFile.filter((FilterFunction<String>) x -> x.split(",").length != 0).map(tidalDataParser, RowEncoder.apply(tidalDataParser.getSchema()));
        String filePath = System.getProperty("java.io.tmpdir") + "tidalData";
        tidalDataset.coalesce(1).write().mode(SaveMode.Overwrite).option("timestampFormat", "yyyy-MM-dd'T'hh:mm:ss").csv(filePath);
    }

    public static String getFilePath(String foldername){
        String filePath = System.getProperty("java.io.tmpdir") + foldername;
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
