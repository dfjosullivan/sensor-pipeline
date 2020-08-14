package com.sensorsimulator;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class AzureConnectorTest {

    @org.junit.jupiter.api.Test
    void getContainer() {
        String storageConnectionString = "DefaultEndpointsProtocol=https;"
                + "AccountName=sensordisplay;"
                + "AccountKey=fnjeNHbag635H5R0yGnzrocrddttLVwWYYSCNyBPsqcf181TZ7aT/Ff0NBu7RNHqi2dxoa2LSmrkQWM4wMk9nA==;";
        CloudBlobContainer actual = null;
        CloudBlobContainer expected = null;
        try {
            AzureConnector connector = new AzureConnector(storageConnectionString);
             actual = connector.getContainer("sensordata");
             expected = CloudStorageAccount.parse(storageConnectionString).createCloudBlobClient().getContainerReference("sensordata");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
        assertEquals(actual.getUri(), expected.getUri());
    }

    @org.junit.jupiter.api.Test
    void getCloudBlockBlob() {
        String storageConnectionString = "DefaultEndpointsProtocol=https;"
                + "AccountName=sensordisplay;"
                + "AccountKey=fnjeNHbag635H5R0yGnzrocrddttLVwWYYSCNyBPsqcf181TZ7aT/Ff0NBu7RNHqi2dxoa2LSmrkQWM4wMk9nA==;";
        AzureConnector ac = new AzureConnector(storageConnectionString);
        StorageUri actual = null;
        StorageUri expected = null;
        try {
             actual = ac.getFile("sensordata", "tidal-data-2019.csv").getStorageUri();
             expected = CloudStorageAccount.parse(storageConnectionString)
                    .createCloudBlobClient().getContainerReference("sensordata")
                    .getBlockBlobReference("tidal-data-2019.csv").getStorageUri();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
        assertEquals(expected, actual);
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
        ArrayList<URI> actual = null;
        ArrayList<URI> expected = new ArrayList<>();
        try {
            actual = ac.getListOfFiles("sensordata");
            for (ListBlobItem blob : CloudStorageAccount.parse(storageConnectionString)
                    .createCloudBlobClient().getContainerReference("sensordata")
                    .listBlobs(null, true)) {
                expected.add(blob.getUri());
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