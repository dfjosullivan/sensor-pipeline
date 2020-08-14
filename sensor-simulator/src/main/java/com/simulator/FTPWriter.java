package com.simulator;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class FTPWriter implements Runnable {
    final String inputFile;
    final String outputFile;
    final String username;
    final String password;
    final String server;
    final int speed;
    final FTPClient ftpClient;
    final boolean localMode;
    final int maxLines;

    public FTPWriter(String inputFile, String outputFile, String username, String password, String server, int speed, boolean local, int maxLines) {
        this.inputFile = inputFile;
        this.username = username;
        this.password = password;
        this.ftpClient = new FTPClient();
        this.outputFile = outputFile;
        this.server = server;
        this.speed = 300000 / speed;
        this.localMode = local;
        this.maxLines = maxLines;
    }

    @Override
    public void run() {

        BufferedInputStream bufferedInputStream =
                null;
        try {
            bufferedInputStream = new BufferedInputStream(new FileInputStream(inputFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        AtomicInteger lineCounts = new AtomicInteger(0);
        try {
            this.ftpClient.connect(server);
            this.ftpClient.login(username, password);
            if(localMode){
                this.ftpClient.enterLocalPassiveMode();
            }

            FTPFile[] filesOnserver = this.ftpClient.listFiles();
            ArrayList<String> listOfFiles = new ArrayList<>();
            for (FTPFile ftpFile : filesOnserver) {
                listOfFiles.add(ftpFile.getName());
            }
            if (!listOfFiles.contains(outputFile)){
                this.ftpClient.storeFile(outputFile, new ByteArrayInputStream(new byte[0]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (BufferedReader br = new BufferedReader(new InputStreamReader(this.ftpClient.retrieveFileStream(outputFile)))) {
            br.lines().forEach(x -> lineCounts.getAndIncrement());
            this.ftpClient.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream));
        bufferedReader.lines().forEach(x -> {
            byte[] line = (x + "\n").getBytes();
            try {
                this.ftpClient.connect(server);
                this.ftpClient.login(username, password);
                if(localMode){
                    this.ftpClient.enterLocalPassiveMode();
                }
                System.out.println(ftpClient.getReplyString());

                if(lineCounts.get() > maxLines){
                    lineCounts.set(0);
                    this.ftpClient.deleteFile(outputFile);
                    this.ftpClient.storeFile(outputFile, new ByteArrayInputStream(line));
                } else {
                    this.ftpClient.appendFile(outputFile, new ByteArrayInputStream(line));
                }
                System.out.println(ftpClient.getReplyString());
                lineCounts.getAndIncrement();
                this.ftpClient.disconnect();
                Thread.sleep(speed);
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        });
    }
}
