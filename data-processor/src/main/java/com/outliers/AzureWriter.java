package com.outliers;

import com.microsoft.azure.storage.StorageException;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

public class AzureWriter implements Runnable {

    final AzureConnector azureConnector;
    final SparkSession ss;
    final String containerName;
    final String server;
    final String username;
    final String password;
    final boolean localMode;

    public AzureWriter(AzureConnector azureConnector, SparkSession ss, String containerName,
                       String server, String username, String password, boolean localMode) {
        this.azureConnector = azureConnector;
        this.ss = ss;
        this.containerName = containerName;
        this.server = server;
        this.username = username;
        this.password = password;
        this.localMode = localMode;
    }

    @Override
    public void run() {
        FileSplitter fs = new FileSplitter(this.username, this.password, this.server, this.localMode, this.ss);

        fs.splitFiles();

        String[] filesToUpload = new String[]{fs.getFilePath("tidalDataset"), fs.getFilePath("windDataset"),
                fs.getFilePath("currentDataset"), fs.getFilePath("atmDataset"), fs.getFilePath("sensorDataset")};

        for (String s : filesToUpload) {
            new File(s).deleteOnExit();
        }


        try {
            azureConnector.write(this.containerName, "outputData/" + this.username + "/tidal-data-2019.csv", filesToUpload[0]);
            System.out.println("Appended data to " + "outputData/" + this.username + "/tidal-data-2019.csv");
            azureConnector.write(this.containerName, "outputData/" + this.username + "/wind-data-2019.csv", filesToUpload[1]);
            System.out.println("Appended data to " + "outputData/" + this.username + "/wind-data-2019.csv");
            azureConnector.write(this.containerName, "outputData/" + this.username + "/current-data-2019.csv", filesToUpload[2]);
            System.out.println("Appended data to " + "outputData/" + this.username + "/current-data-2019.csv");
            azureConnector.write(this.containerName, "outputData/" + this.username + "/atmospheric-data-2019.csv", filesToUpload[3]);
            System.out.println("Appended data to " + "outputData/" + this.username + "/atmospheric-data-2019.csv");
            azureConnector.write(this.containerName, "outputData/" + this.username + "/sensor-data-2019.csv", filesToUpload[4]);
            System.out.println("Appended data to " + "outputData/" + this.username + "/sensor-data-2019.csv");
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
