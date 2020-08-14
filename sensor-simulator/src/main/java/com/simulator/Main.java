package com.simulator;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        int users = 0;
        for(String s : args){
            if (s.startsWith("--user")) {
                users++;
            }
        }
        String[][] creds = new String[users][2];
        boolean localMode = true;
        String server = "52.187.252.165";
        int maxNumberOfLines = 100;
        int speed = 1;
        String containerName = "sensordata";
        boolean replaceTime = false;
        String storageConnectionString = "DefaultEndpointsProtocol=https;"
                + "AccountName=sensordisplay;"
                + "AccountKey=fnjeNHbag635H5R0yGnzrocrddttLVwWYYSCNyBPsqcf181TZ7aT/Ff0NBu7RNHqi2dxoa2LSmrkQWM4wMk9nA==;";

        for (String s : args) {
            if (s.startsWith("--user")) {
                String[] arr = s.split("=|,");
                creds[ArrayUtils.indexOf(args, s)][0] = arr[1];
                creds[ArrayUtils.indexOf(args, s)][1] = arr[2];
                continue;
            }
            String value = s.substring(s.indexOf("=") + 1);

            if (s.startsWith("--server")) {
                server = value;
                continue;
            }
            if (s.startsWith("--localMode")) {
                localMode = Boolean.parseBoolean(value);
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
            if (s.startsWith("--speed")) {
                speed = Integer.parseInt(value);
            }
            if(s.startsWith("--maxNumberOfLines")){
                maxNumberOfLines = Integer.parseInt(value);
            }
            if (s.startsWith("--replaceDateTime")) {
                replaceTime = Boolean.parseBoolean(value);
            }
        }

        if(localMode){
            server = "localhost";
        }
        if (creds.length == 0) {
            throw new IllegalArgumentException("At least one user must be specified");
        }

        SparkSession ss = SparkSession.builder()
                .master("local")
                .appName("Sensor simulation")
                .getOrCreate();
        AzureConnector azureConnector = new AzureConnector(storageConnectionString);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        FileProcessor fp = new FileProcessor(azureConnector, containerName);
        fp.processTidalDataset(ss, replaceTime);
        fp.processElseDataset(ss, replaceTime);

        FTPWriter task1 = new FTPWriter(fp.getFilePath("combinedData"), "everything-else-2019.csv", creds[0][0], creds[0][1], server, speed, localMode, maxNumberOfLines);
        FTPWriter task2 = new FTPWriter(fp.getFilePath("combinedData"), "everything-else-2019.csv",creds[1][0], creds[1][1], server, speed, localMode, maxNumberOfLines);
        FTPWriter task3 = new FTPWriter(fp.getFilePath("combinedData"), "everything-else-2019.csv", creds[2][0], creds[2][1], server, speed, localMode, maxNumberOfLines);
        FTPWriter task4 = new FTPWriter(fp.getFilePath("tidalData"), "tidal-data-2019.csv", creds[0][0], creds[0][1],server, speed, localMode, maxNumberOfLines);
        FTPWriter task5 = new FTPWriter(fp.getFilePath("tidalData"), "tidal-data-2019.csv",creds[1][0], creds[1][1],server, speed, localMode, maxNumberOfLines);
        FTPWriter task6 = new FTPWriter(fp.getFilePath("tidalData"), "tidal-data-2019.csv",creds[2][0], creds[2][1],server, speed, localMode, maxNumberOfLines);

        executorService.submit(task1);
        executorService.submit(task2);
        executorService.submit(task3);
        executorService.submit(task4);
        executorService.submit(task5);
        executorService.submit(task6);

        executorService.shutdown();
        try {
            executorService.awaitTermination(200, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
