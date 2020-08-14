package com.simulator;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;

public class AzureConnector {

    CloudStorageAccount storageAccount;
    CloudBlobClient blobClient;


    public AzureConnector(String connestionString) {
        try {
            this.storageAccount = CloudStorageAccount.parse(connestionString);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
        this.blobClient = storageAccount.createCloudBlobClient();

    }

    public CloudBlobContainer getContainer(String containerName) throws URISyntaxException, StorageException {
        CloudBlobContainer container = this.blobClient.getContainerReference(containerName);
        return container;
    }

    public CloudBlockBlob getFile(String containerName, String fileName) throws URISyntaxException, StorageException {
        CloudBlobContainer container = this.blobClient.getContainerReference(containerName);
        CloudBlockBlob cloudBlockBlob = container.getBlockBlobReference(fileName);
        return cloudBlockBlob;
    }

    public BlobInputStream getBlobInputStream(String containerName, String fileName) throws URISyntaxException, StorageException {
        CloudBlobContainer container = this.blobClient.getContainerReference(containerName);
        CloudBlockBlob cloudBlockBlob = container.getBlockBlobReference(fileName);
        BlobInputStream blobInputStream = cloudBlockBlob.openInputStream();
        return blobInputStream;
    }

    public BlobOutputStream getBlobOutputStream(String containerName, String fileName) throws StorageException, URISyntaxException {
        CloudBlobContainer container = this.blobClient.getContainerReference(containerName);
        CloudBlockBlob cloudBlockBlob = container.getBlockBlobReference(fileName);
        BlobOutputStream blobOutputStream = cloudBlockBlob.openOutputStream();
        return blobOutputStream;
    }

    public ArrayList<String> getListOfContainers() {
        ArrayList<String> list = new ArrayList<>();
        this.blobClient.listContainers().forEach(x -> list.add(x.getName()));
        return list;
    }

    public ArrayList<URI> getListOfFiles(String containerName) throws URISyntaxException, StorageException {
        CloudBlobContainer container = this.blobClient.getContainerReference(containerName);
        ArrayList<URI> res = new ArrayList<>();
        for (ListBlobItem blobItem : container.listBlobs(null, true)) {
            res.add(blobItem.getUri());
        }
        return res;
    }
}
