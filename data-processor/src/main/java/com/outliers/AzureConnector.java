package com.outliers;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;

public class AzureConnector {

    CloudStorageAccount storageAccount;
    CloudBlobClient blobClient;


    public AzureConnector(String connectionString) {
        try {
            this.storageAccount = CloudStorageAccount.parse(connectionString);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
        this.blobClient = storageAccount.createCloudBlobClient();

    }

    public void uploadFile(String containerName, String fileName, String outputFileName) throws URISyntaxException, StorageException, IOException {
        CloudBlobContainer container = this.blobClient.getContainerReference(containerName);
        container.getBlockBlobReference(outputFileName).uploadFromFile(fileName);
    }

    public void write(String container, String toFile, String fromFile) throws URISyntaxException, StorageException, IOException {
        CloudBlobContainer cn = this.blobClient.getContainerReference(container);
        CloudAppendBlob blob = cn.getAppendBlobReference(toFile);
        if(!blob.exists()){
            blob.createOrReplace();
        }
        blob.appendFromFile(fromFile);
    }

    public ArrayList<String> getListOfContainers() {
        ArrayList<String> list = new ArrayList<>();
        this.blobClient.listContainers().forEach(x -> list.add(x.getName()));
        return list;
    }


    public ArrayList<String> getListOfFiles(String containerName) throws URISyntaxException, StorageException {
        CloudBlobContainer container = this.blobClient.getContainerReference(containerName);
        ArrayList<String> res = new ArrayList<>();
        for (ListBlobItem blobItem : container.listBlobs(null, true)) {
            String blob = blobItem.getUri().toString();
            res.add(blob.substring(blob.indexOf("/",blob.indexOf(containerName))+1));
        }
        return res;
    }
}
