package ch.epfl.tkvs.yarn.appmaster;

import ch.epfl.tkvs.config.NetConfig;
import ch.epfl.tkvs.yarn.Utils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class AppMaster implements AMRMClientAsync.CallbackHandler {

    private static Logger log = Logger.getLogger(AppMaster.class.getName());
    private static String hostname;

    private YarnConfiguration conf;

    private static final int MAX_NUMBER_OF_WORKERS = 10;

    private NMClient nmClient;
    AMRMClientAsync<ContainerRequest> rmClient;

    private int containerCount = 0;

    private ServerSocket server;

    public static void main(String[] args) {

        try {
            log.info("Initializing...");

            // Compute hostname
            hostname = InetAddress.getLocalHost().getHostName();
            log.info("AppMaster hostname: " + hostname);

            new AppMaster().run();
        } catch (Exception ex) {
            log.fatal("Could not run yarn app master", ex);
        }
    }

    public void run() throws Exception {
        conf = new YarnConfiguration();

        // Create AM Socket
        server = new ServerSocket(NetConfig.AM_DEFAULT_PORT);

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

        // Request Containers from RM
        NetConfig conf = new NetConfig();
        Map<String, Integer> tmHosts = conf.getTMs();

        for (String host : tmHosts.keySet()) {
            log.info("Requesting Container at " + host);
            rmClient.addContainerRequest(new ContainerRequest(capability, new String[] { host }, null, priority));
            containerCount += 1;
        }

        log.info("Starting server at " + hostname + ":" + NetConfig.AM_DEFAULT_PORT);
        conf.writeAppMasterHostname(hostname); // Write its host name to HDFS
        ExecutorService threadPool = Executors.newFixedThreadPool(MAX_NUMBER_OF_WORKERS);

        while (!server.isClosed() && containerCount > 0) {
            try {
                log.info("Waiting for message...");
                Socket sock = server.accept();
                log.info("Processing message...");

                BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                String input = in.readLine();

                switch (input) {
                case ":exit":
                    log.info("Stopping Server");
                    sock.close();
                    server.close();

                    log.info("Stopping TMs");
                    for (Entry<String, Integer> ent : tmHosts.entrySet()) {
                        Socket exitSock = new Socket(ent.getKey(), ent.getValue());
                        PrintWriter out = new PrintWriter(exitSock.getOutputStream(), true);
                        out.println(input);
                        out.close();
                        exitSock.close();
                    }
                    break;
                default:
                    threadPool.execute(new AMWorker(input, sock));
                }

            } catch (IOException e) {
                log.error("sock.accept ", e);
            }
        }

        while (containerCount > 0) {
            Thread.sleep(1000);
        }

        log.info("Unregistered");
        nmClient.stop();
        server.close();
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        rmClient.stop();
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {
                nmClient.startContainer(container, initContainer());
                log.info("Container launched " + container.getId());
            } catch (Exception ex) {
                log.error("Container not launched " + container.getId(), ex);
            }
        }
    }

    private ContainerLaunchContext initContainer() {
        try {
            // Create Container Context
            ContainerLaunchContext cCLC = Records.newRecord(ContainerLaunchContext.class);
            cCLC.setCommands(Collections.singletonList("$JAVA_HOME/bin/java " + Utils.TM_XMX + " ch.epfl.tkvs.transactionmanager.TransactionManager " + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

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
                --containerCount;
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
        // TODO: FIND HOW TO CALL IT!!!
    }

    @Override
    public float getProgress() {
        return 0;
    }
}
