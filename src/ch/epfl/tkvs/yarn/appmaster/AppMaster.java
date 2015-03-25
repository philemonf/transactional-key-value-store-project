package ch.epfl.tkvs.yarn.appmaster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import ch.epfl.tkvs.yarn.Utils;


public class AppMaster implements AMRMClientAsync.CallbackHandler {

    private static Logger log = Logger.getLogger(AppMaster.class.getName());
    private YarnConfiguration conf;

    private NMClient nmClient;
    AMRMClientAsync<ContainerRequest> rmClient;

    private int containerCount = 0;
    private int containersAllocated = 0;

    public static int port = 24744;
    private boolean listening = true;

    public static void main(String[] args) {
        try {
            log.info("Initializing...");
            new AppMaster().run();
        } catch (Exception ex) {
            log.fatal("Could not run yarn app master", ex);
        }
    }

    public void run() throws Exception {
        conf = new YarnConfiguration();

        // Create NM Client
        nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Create AM - RM Client
        rmClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
        rmClient.init(conf);
        rmClient.start();

        // Register with RM
        rmClient.registerApplicationMaster("", 0, "");
        log.info("Registered");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Reqiest Containers from RM
        Path slavesPath = new Path(Utils.TKVS_CONFIG_PATH, "slaves");
        FileSystem fs = slavesPath.getFileSystem(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(slavesPath)));

        String slave = reader.readLine();
        while (slave != null) {
            log.info("Requesting Container at " + slave);
            rmClient.addContainerRequest(new ContainerRequest(capability, new String[] { slave }, null, priority));

            ++containerCount;
            slave = reader.readLine();
        }

        while (!containersFinished()) {
            Thread.sleep(100);
        }

        log.info("Unregistered");
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        nmClient.stop();
        rmClient.stop();
    }

    private boolean containersFinished() {
        return containerCount == 0;
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {
                nmClient.startContainer(container, initContainer());
                log.info("Container launched " + container.getId());

                // TODO: Fix this part of AppMaster!
                synchronized (this) {
                    containersAllocated++;
                    if (containerCount == containersAllocated) {
                        log.info("Starting AMServer");
                        startServer();
                    }
                }
            } catch (Exception ex) {
                log.error("Container not launched " + container.getId(), ex);
            }
        }
    }

    private ContainerLaunchContext initContainer() {
        try {
            // Create Container Context
            ContainerLaunchContext cCLC = Records.newRecord(ContainerLaunchContext.class);
            cCLC.setCommands(Collections
                    .singletonList("$JAVA_HOME/bin/java ch.epfl.tkvs.transactionmanager.TransactionManager"));

            // Set Container jar
            LocalResource jar = Records.newRecord(LocalResource.class);
            Utils.setUpLocalResource(Utils.TKVS_JAR_PATH, jar, conf);
            cCLC.setLocalResources(Collections.singletonMap(Utils.TKVS_JAR_NAME, jar));

            // Set Container CLASSPATH
            Map<String, String> env = new HashMap<String, String>();
            Utils.setUpEnv(env, conf);
            cCLC.setEnvironment(env);

            return cCLC;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statusOfContainers) {
        for (ContainerStatus status : statusOfContainers) {
            log.info("Container finished " + status.getContainerId());
            synchronized (this) {
                containerCount--;
            }
        }
    }

    private void startServer() {
        log.info("Starting server...");
        try {
            ServerSocket sock = new ServerSocket(port);
            while (listening) {
                new AMThread(sock.accept(), log).start();
            }
            sock.close();
        } catch (IOException e) {
            log.fatal("Could not listen on port " + e);
            try {
                log.fatal("Unregistered");
                rmClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, "", "");
                nmClient.stop();
                rmClient.stop();
            } catch (Exception ex) {
                log.fatal("Unreg. failed", ex);
            }
        }
    }

    @Override
    public void onError(Throwable e) {
        log.error("onError", e);
    }

    @Override
    public void onNodesUpdated(List<NodeReport> nodeReports) {
    }

    @Override
    public void onShutdownRequest() {
        log.warn("onShutdownRequest");
    }

    @Override
    public float getProgress() {
        return 0;
    }
}
