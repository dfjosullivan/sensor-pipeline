package com.simulator;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class FileProcessorTest {

    @Test
    void processElseDataset() {
    }

    @Test
    void processTidalDataset() {
    }

    @Test
    void getFilePath() {
        File file = null;
        try {
            File dir = new File(System.getProperty("java.io.tmpdir") + "tidalData");
            FileUtils.cleanDirectory(dir);
            file = new File(dir.toString()+"/testFile.csv");
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String resultPath = FileProcessor.getFilePath("tidalData");
        assertEquals(resultPath, file.toString());
    }
}