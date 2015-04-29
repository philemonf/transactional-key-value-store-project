package ch.epfl.tkvs.yarn.appmaster;

import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
import ch.epfl.tkvs.yarn.RoutingTable;
import ch.epfl.tkvs.yarn.Utils;
import org.apache.hadoop.net.NetUtils;
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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class AppMaster implements AMRMClientAsync.CallbackHandler {

    private static Logger log = Logger.getLogger(AppMaster.class.getName());

    private YarnConfiguration conf;
    private static final int MAX_NUMBER_OF_WORKERS = 10;
    private NMClient nmClient;
    AMRMClientAsync<ContainerRequest> rmClient;
    private RoutingTable routing;

    private int activeTMCount = 0;

    public static void main(String[] args) {
        try {
            log.info("Initializing...");
            new AppMaster().run();
        } catch (Exception ex) {
            log.fatal("Failed", ex);
        }
        log.info("Finished");
        System.exit(0);
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

        // Get port and hostname.
        String amIP = Utils.extractIP(NetUtils.getHostname());
        int amPort = NetUtils.getFreeSocketPort();
        routing = new RoutingTable(amIP, amPort);

        // Register with RM and create AM Socket
        rmClient.registerApplicationMaster(amIP, amPort, "");
        log.info("Registered and starting server at " + amIP + ":" + amPort);
        Utils.writeAMAddress(amIP + ":" + amPort);
        ServerSocket server = new ServerSocket(amPort);

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Request Containers from RM
        ArrayList<String> tmRequests = Utils.readTMHostnames();
        HashMap<String, ContainerRequest> contRequests = new HashMap<>();
        for (String tmIp : tmRequests) {
            log.info("Requesting Container at " + tmIp);
            ContainerRequest req = new ContainerRequest(capability, new String[] { tmIp }, null, priority);
            contRequests.put(tmIp, req);
            rmClient.addContainerRequest(req);
        }

        // Get TM network info from all TMs.
        server.setSoTimeout(3000); // 3 seconds accept() timeout.
        try {
            log.info("Waiting for a reply from all TMs");
            while (routing.size() < tmRequests.size()) {
                Socket sock = server.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                String input = in.readLine();
                String[] info = input.split(":");
                log.info("Registering TM at " + info[0] + ":" + info[1]);
                routing.addTM(info[0], Integer.parseInt(info[1]));
            }
            log.info("All TMs replied successfully");
        } catch (SocketTimeoutException e) {
            log.warn("Did not get reply from all TMs");
            for (String tmIp : tmRequests) {
                if (!routing.contains(tmIp)) {
                    log.warn("TM at " + tmIp + " did not reply");
                    rmClient.removeContainerRequest(contRequests.get(tmIp));
                }
            }
        }
        server.setSoTimeout(0); // reset timeout.

        log.info("Sending routing information to TMs");
        String routingEncoded = Base64Utils.convertToBase64(routing);
        for (Entry<String, Integer> tm : routing.getTMs().entrySet()) {
            Socket sock = new Socket(tm.getKey(), tm.getValue());
            PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
            out.print(routingEncoded);
            out.close();
            sock.close();
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(MAX_NUMBER_OF_WORKERS);
        while (!server.isClosed() && activeTMCount > 0) {
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
                    for (Entry<String, Integer> ent : routing.getTMs().entrySet()) {
                        Socket exitSock = new Socket(ent.getKey(), ent.getValue());
                        PrintWriter out = new PrintWriter(exitSock.getOutputStream(), true);
                        out.println(input);
                        out.close();
                        exitSock.close();
                    }
                    break;
                default:
                    threadPool.execute(new AMWorker(routing, input, sock));
                }

            } catch (IOException e) {
                log.error("sock.accept ", e);
            }
        }

        while (activeTMCount > 0) {
            Thread.sleep(1000);
        }

        server.close();
        log.info("Unregistering");
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            String ip = Utils.extractIP(container.getNodeHttpAddress());
            if (!routing.contains(ip)) {
                synchronized (this) {
                    ++activeTMCount;
                }
                try {
                    nmClient.startContainer(container, initContainer());
                    log.info("Container launched " + container.getId());
                } catch (Exception ex) {
                    log.error("Container not launched " + container.getId(), ex);
                }
            }
        }
    }

    private ContainerLaunchContext initContainer() throws Exception {
        // Create Container Context
        ContainerLaunchContext cCLC = Records.newRecord(ContainerLaunchContext.class);
        cCLC.setCommands(Collections.singletonList("$JAVA_HOME/bin/java " + Utils.TM_XMX + " ch.epfl.tkvs.transactionmanager.TransactionManager " + "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

        // Set Container jar
        LocalResource jar = Records.newRecord(LocalResource.class);
        Utils.setUpLocalResource(Utils.TKVS_JAR_PATH, jar, conf);
        cCLC.setLocalResources(Collections.singletonMap(Utils.TKVS_JAR_NAME, jar));

        // Set Container CLASSPATH
        Map<String, String> env = new HashMap<String, String>();
        Utils.setUpEnv(env, conf);
        env.put("AM_IP", routing.getAMIp());
        env.put("AM_PORT", String.valueOf(routing.getAMPort()));
        cCLC.setEnvironment(env);

        return cCLC;
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statusOfContainers) {
        for (ContainerStatus status : statusOfContainers) {
            log.info("Container finished " + status.getContainerId());
            synchronized (this) {
                --activeTMCount;
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
