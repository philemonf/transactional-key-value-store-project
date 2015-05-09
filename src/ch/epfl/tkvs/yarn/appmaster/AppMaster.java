package ch.epfl.tkvs.yarn.appmaster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.ExitMessage;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.TMInitMessage;
import static ch.epfl.tkvs.yarn.HDFSLogger.TKVS_LOGS_PATH;
import ch.epfl.tkvs.yarn.RemoteTransactionManager;
import ch.epfl.tkvs.yarn.RoutingTable;
import ch.epfl.tkvs.yarn.Utils;
import ch.epfl.tkvs.yarn.appmaster.centralized_decision.DeadlockCentralizedDecider;
import ch.epfl.tkvs.yarn.appmaster.centralized_decision.ICentralizedDecider;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * The YARN Application Master is responsible for launching containers that contain Transaction Managers (TM). Prepares
 * the TM containers and launches the process in each. As soon as TMs reply when they are ready, the AM listens for
 * messages that have to do with centralized control. On exit, it gracefully stops all active TMs.
 * @see ch.epfl.tkvs.transactionmanager.TransactionManager
 * @see ch.epfl.tkvs.yarn.appmaster.AMWorker
 * @see ch.epfl.tkvs.yarn.RoutingTable
 */
public class AppMaster {

    private final static Logger log = Logger.getLogger(AppMaster.class.getName());

    private static RMCallbackHandler rmHandler;
    private static AMRMClientAsync<ContainerRequest> rmClient;
    private static int nextXid = 0;

    public static void main(String[] args) {
        Utils.initLogLevel();
        log.info("Initializing...");
        try {
            Path logDir = new Path(TKVS_LOGS_PATH);
            FileSystem fs = logDir.getFileSystem(new YarnConfiguration());
            fs.delete(logDir, true); // delete old log dir.

            new AppMaster().run();
        } catch (Exception ex) {
            log.fatal("Failed", ex);
        }
        log.info("Finished");
        System.exit(0);
    }

    public void run() throws Exception {
        YarnConfiguration conf = new YarnConfiguration();

        // Create NM Client.
        log.info("Creating NM client");
        NMClientAsync nmClient = new NMClientAsyncImpl(new NMCallbackHandler());
        nmClient.init(conf);
        nmClient.start();

        // Create RM Client.
        log.info("Creating RM client");
        rmHandler = new RMCallbackHandler(nmClient, conf);
        rmClient = new AMRMClientAsyncImpl<>(1000, rmHandler);
        rmClient.init(conf);
        rmClient.start();

        // Get ip and port.
        String amIp = Utils.extractIP(NetUtils.getHostname());
        int amPort = NetUtils.getFreeSocketPort();

        // Initialize the routing table with AM's ip:port.
        rmHandler.setRoutingTable(new RoutingTable(amIp, amPort));

        // Register with RM and create AM Socket.
        rmClient.registerApplicationMaster(amIp, amPort, null);
        log.info("Registered and starting server at " + amIp + ":" + amPort);
        ServerSocket server = new ServerSocket(amPort);

        // Request Containers from RM.
        ArrayList<String> tmRequests = Utils.readTMHostnames();
        log.info("All TM request: " + tmRequests);

        HashMap<String, List<ContainerRequest>> contRequests = new HashMap<>();
        for (String tmIp : tmRequests) {
            ContainerRequest req = requestContainer(tmIp);
            if (!contRequests.containsKey(tmIp)) {
                contRequests.put(tmIp, new LinkedList<ContainerRequest>());
            }
            contRequests.get(tmIp).add(req);
        }

        // Get TM network info from all TMs.
        server.setSoTimeout(10000); // 10 seconds accept() timeout.
        try {
            log.info("Waiting for a reply from all TMs");

            // To avoid infinite hang due to pings from the client
            // we keep a counter that indicates us how many subsequent
            // pings occurred.
            int pingCount = 0;

            log.info("Waiting for " + contRequests.size() + " TM to be registered.");
            while (rmHandler.getRoutingTable().size() < contRequests.size() || pingCount < 3) {
                Socket sock = server.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                String input = in.readLine();
                if (input.equals(":ping")) {
                    log.info("Receive a premature ping from the client.");
                    PrintWriter out = new PrintWriter(sock.getOutputStream());
                    out.println("not ready");
                    out.flush();
                    out.close();
                    ++pingCount;
                } else {
                    pingCount = 0;
                    String[] info = input.split(":");
                    log.info("Registering TM at " + info[0] + ":" + info[1]);
                    rmHandler.getRoutingTable().addTM(new RemoteTransactionManager(info[0], Integer.parseInt(info[1])));
                }

                sock.close();
            }
            log.info("All TMs replied successfully");
        } catch (SocketTimeoutException e) {
            log.warn("Did not get reply from all TMs");
            for (String tmIp : tmRequests) {
                if (!rmHandler.getRoutingTable().contains(tmIp)) {
                    // FIXME This might have unexpected behavior in case of local
                    // testing where more than one TM has the same ip
                    // since TM that have responded may be removed
                    // but this only concerns testing since the system
                    // is not supposed to support multiple TM on a node at the moment
                    log.warn("TM at " + tmIp + " did not reply");
                    for (ContainerRequest req : contRequests.get(tmIp)) {
                        rmClient.removeContainerRequest(req);
                    }
                }
            }
        }
        server.setSoTimeout(0); // reset timeout.

        // Send the routing information to all TMs.
        log.info("Sending routing information to TMs");
        TMInitMessage initMessage = new TMInitMessage(rmHandler.getRoutingTable());
        for (RemoteTransactionManager tm : rmHandler.getRoutingTable().getTMs()) {
            tm.sendMessage(initMessage, false);
        }

        // Start listening to messages.
        ICentralizedDecider decider = new DeadlockCentralizedDecider(); // TODO make it configurable
        ExecutorService threadPool = Executors.newCachedThreadPool();
        while (!server.isClosed() && rmHandler.getContainerCount() > 0) {
            try {
                log.info("Waiting for message...");
                Socket sock = server.accept();
                log.info("Processing message...");

                BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                String input = in.readLine();

                switch (input) {
                case ":ping": {
                    log.info("Receive a ping");
                    PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
                    out.println("ok");
                    out.close();
                    break;
                }

                case ":exit":
                    log.info("Stopping Server");
                    sock.close();
                    server.close();

                    // On exit, stop all TM containers.
                    log.info("Stopping TMs");
                    ExitMessage exitMessage = new ExitMessage();
                    for (RemoteTransactionManager tm : rmHandler.getRoutingTable().getTMs()) {
                        tm.sendMessage(exitMessage, false);
                    }
                    break;
                default:
                    try {
                        JSONObject jsonRequest = new JSONObject(input);
                        threadPool.execute(new AMWorker(rmHandler.getRoutingTable(), jsonRequest, sock, decider));
                    } catch (JSONException e) {
                        log.warn("Non JSON message will not be parsed: " + input);
                    }
                }

            } catch (IOException e) {
                log.error("sock.accept ", e);
            }
        }

        // On exit, wait for all containers to stop.
        int waitIterationElapsed = 0;
        while (rmHandler.getContainerCount() > 0 && waitIterationElapsed < 10) {
            log.info("Still " + rmHandler.getContainerCount() + " containers need to be closed.");
            Thread.sleep(1000);
            ++waitIterationElapsed;
        }

        if (rmHandler.getContainerCount() > 0) {
            log.info("Force to close containers that are not closed.");
            rmHandler.closeAllRegisteredContainers();

            while (rmHandler.getContainerCount() > 0) {
                log.info("Still " + rmHandler.getContainerCount() + " containers need to be closed.");
                Thread.sleep(1000);
            }

        }

        // Stop remaining resources and unregister application.
        nmClient.stop();
        server.close();

        log.info("Unregistering");
        try {
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", null);
        } catch (Exception e) {
            log.error("Failed to unregister application", e);
        }
        rmClient.stop();
    }

    /**
     * /** Send a message to a transaction manager (TM).
     * @param localityHash the locality hash of the node running the TM
     * @param message the message to send
     * @param shouldWait whether one should wait for a response
     * @return the response or null if !shouldWait
     * @throws IOException in case of network failure or bad message format
     */
    public static JSONObject sendMessageToTM(int localityHash, Message message, boolean shouldWait) throws IOException {
        return rmHandler.getRoutingTable().findTM(localityHash).sendMessage(message, shouldWait);
    }

    public static int numberOfRegisteredTMs() {
        return rmHandler.getContainerCount();
    }

    public static synchronized int nextTransactionId() {
        int xid = nextXid;
        nextXid++;
        return xid;
    }

    private static ContainerRequest requestContainer(String ip) {
        log.info("Requesting Container at " + ip);
        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(Utils.TM_MEMORY);
        capability.setVirtualCores(Utils.TM_CORES);

        ContainerRequest req = new ContainerRequest(capability, new String[] { ip }, null, priority);
        rmClient.addContainerRequest(req);
        return req;
    }

}