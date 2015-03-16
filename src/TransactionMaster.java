package com.hortonworks.simpleyarnapp;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

/**
 * The TransactionMaster is the ApplicationMaster of this application.
 * It is started by the ResourceManager on client's request.
 * Then, it receives command directly from the client.
 */
public class TransactionMaster implements AMRMClientAsync.CallbackHandler {
    Configuration configuration;
    String command;

    public TransactionMaster(String command) {
        this.command = command;
        this.configuration = new YarnConfiguration();
    }

    public void onContainersAllocated(List<Container> containers) {
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
    }

    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onReboot() {
    }

    public void onShutdownRequest() {
    }

    public void onError(Throwable t) {
    }

    public float getProgress() {
        return 0;
    }

    public boolean doneWithContainers() {
        return true;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        final String command = args[0];

        TransactionMaster master = new TransactionMaster(command);
        master.runMainLoop();
    }

    public void runMainLoop() throws Exception {
    }
}
