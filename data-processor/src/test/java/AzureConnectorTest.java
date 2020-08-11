import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.outliers.AzureConnector;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class AzureConnectorTest {

    @org.junit.jupiter.api.Test
    void uploadFile() {
        String storageConnectionString = "DefaultEndpointsProtocol=https;"
                + "AccountName=sensordisplay;"
                + "AccountKey=fnjeNHbag635H5R0yGnzrocrddttLVwWYYSCNyBPsqcf181TZ7aT/Ff0NBu7RNHqi2dxoa2LSmrkQWM4wMk9nA==;";
        AzureConnector ac = new AzureConnector(storageConnectionString);
        CloudBlobContainer container = null;
        try {
            container = CloudStorageAccount.parse(storageConnectionString)
                    .createCloudBlobClient().getContainerReference("sensordata");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
        File fileToWrite = new File(System.getProperty("java.io.tempdir")+"uploadTestFile.csv");
        ArrayList<String> existingFiles = new ArrayList<>();
        ArrayList<String> uploadedFiles = new ArrayList<>();
        uploadedFiles.add("uploadTestFile.csv");
        try {
            fileToWrite.createNewFile();
            fileToWrite.deleteOnExit();
            ac.uploadFile("sensordata", fileToWrite.getAbsolutePath(), "tests/uploadTestFile.csv");
            for (ListBlobItem file : container.listBlobs(null, true)) {
                String file_ = file.getUri().toString();
                if(file_.contains("tests")) {
                    existingFiles.add(file_.substring(file_.indexOf("/", file_.indexOf("tests")) + 1));
                }
            }
            container.getBlockBlobReference("tests/uploadTestFile.csv").delete();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        assertArrayEquals(existingFiles.toArray(), uploadedFiles.toArray());

    }

    @org.junit.jupiter.api.Test
    void write() {
        String storageConnectionString = "DefaultEndpointsProtocol=https;"
                + "AccountName=sensordisplay;"
                + "AccountKey=fnjeNHbag635H5R0yGnzrocrddttLVwWYYSCNyBPsqcf181TZ7aT/Ff0NBu7RNHqi2dxoa2LSmrkQWM4wMk9nA==;";
        AzureConnector ac = new AzureConnector(storageConnectionString);
        CloudBlobContainer container = null;
        try {
            container = CloudStorageAccount.parse(storageConnectionString)
                    .createCloudBlobClient().getContainerReference("sensordata");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
        File fileToWrite = new File(System.getProperty("java.io.tempdir")+"writeTestFile.csv");
        ArrayList<String> existingFiles = new ArrayList<>();
        ArrayList<String> testFiles = new ArrayList<>();
        testFiles.add("writeTestFile.csv");
        try {
            fileToWrite.createNewFile();
            fileToWrite.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            ac.write("sensordata", "tests/writeTestFile.csv", fileToWrite.getAbsolutePath());
            for (ListBlobItem file : container
                    .listBlobs(null, true)) {
                String file_ = file.getUri().toString();
                if(file_.contains("tests")) {
                    existingFiles.add(file_.substring(file_.indexOf("/", file_.indexOf("tests")) + 1));
                }
            }
            container.getBlockBlobReference("tests/writeTestFile.csv").delete();

        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertArrayEquals(existingFiles.toArray(), testFiles.toArray());
    }

    @org.junit.jupiter.api.Test
    void getListOfContainers() {
        String storageConnectionString = "DefaultEndpointsProtocol=https;"
                + "AccountName=sensordisplay;"
                + "AccountKey=fnjeNHbag635H5R0yGnzrocrddttLVwWYYSCNyBPsqcf181TZ7aT/Ff0NBu7RNHqi2dxoa2LSmrkQWM4wMk9nA==;";
        AzureConnector ac = new AzureConnector(storageConnectionString);
        ArrayList<String> actual = null;
        ArrayList<String> expected = new ArrayList<>();
        try {
            actual = ac.getListOfContainers();
            for (CloudBlobContainer container : CloudStorageAccount.parse(storageConnectionString)
                    .createCloudBlobClient().listContainers()) {
                expected.add(container.getName());
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @org.junit.jupiter.api.Test
    void getListOfFiles() {
        String storageConnectionString = "DefaultEndpointsProtocol=https;"
                + "AccountName=sensordisplay;"
                + "AccountKey=fnjeNHbag635H5R0yGnzrocrddttLVwWYYSCNyBPsqcf181TZ7aT/Ff0NBu7RNHqi2dxoa2LSmrkQWM4wMk9nA==;";
        AzureConnector ac = new AzureConnector(storageConnectionString);
        ArrayList<String> actual = null;
        ArrayList<String> expected = new ArrayList<>();
        try {
            actual = ac.getListOfFiles("sensordata");
            for (ListBlobItem file : CloudStorageAccount.parse(storageConnectionString)
                    .createCloudBlobClient().getContainerReference("sensordata")
                    .listBlobs(null, true)) {
                String file_ = file.getUri().toString();
                expected.add(file_.substring(file_.indexOf("/",file_.indexOf("sensordata"))+1));
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
        assertArrayEquals(actual.toArray(), expected.toArray());
    }
}