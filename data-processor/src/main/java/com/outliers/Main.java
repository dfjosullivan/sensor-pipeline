package com.outliers;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        String server = "52.187.252.165";
        int frequency = 5;
        String containerName = "sensordata";
        String storageConnectionString = "DefaultEndpointsProtocol=https;"
                + "AccountName=sensordisplay;"
                + "AccountKey=fnjeNHbag635H5R0yGnzrocrddttLVwWYYSCNyBPsqcf181TZ7aT/Ff0NBu7RNHqi2dxoa2LSmrkQWM4wMk9nA==;"
                + "EndpointSuffix=core.windows.net";

        SparkSession ss = SparkSession.builder()
                .master("local")
                .appName("Outlier Calculator")
                .getOrCreate();

        int users = 0;
        for (String s : args) {
            if (s.startsWith("--user")) {
                users++;
            }
        }
        String[][] creds = new String[users][2];
        boolean localMode = true;
        for (String s : args) {
            String value = s.substring(s.indexOf("=") + 1);
            if (s.startsWith("--user")) {
                String[] arr = s.split("=|,");
                creds[ArrayUtils.indexOf(args, s)][0] = arr[1];
                creds[ArrayUtils.indexOf(args, s)][1] = arr[2];
                continue;
            }
            if (s.startsWith("--localMode")) {
                localMode = Boolean.parseBoolean(value);
                continue;
            }
            if (s.startsWith("--frequency")) {
                frequency = Integer.parseInt(value);
                continue;
            }
            if (s.startsWith("--server")) {
                server = value;
                continue;
            }
            if (s.startsWith("--container")) {
                containerName = value;
                continue;
            }
            if (s.startsWith("--connectionString")) {
                storageConnectionString = value;
                continue;
            }
        }
        if (localMode) {
            server = "localhost";
        }

        AzureConnector azureConnector = new AzureConnector(storageConnectionString);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
        AzureWriter task1 = new AzureWriter(azureConnector, ss, containerName, server, creds[0][0], creds[0][1], localMode);
        AzureWriter task2 = new AzureWriter(azureConnector, ss, containerName, server, creds[1][0], creds[1][1], localMode);
        AzureWriter task3 = new AzureWriter(azureConnector, ss, containerName, server, creds[2][0], creds[2][1], localMode);

        executorService.scheduleAtFixedRate(task1, 0, frequency, TimeUnit.MINUTES);
        executorService.scheduleAtFixedRate(task2, 0, frequency, TimeUnit.MINUTES);
        executorService.scheduleAtFixedRate(task3, 0, frequency, TimeUnit.MINUTES);

        try {
            executorService.awaitTermination(24, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
