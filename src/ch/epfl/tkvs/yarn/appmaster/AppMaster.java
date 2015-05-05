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
import java.util.Map.Entry;
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
import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
import ch.epfl.tkvs.yarn.RemoteTransactionManager;
import ch.epfl.tkvs.yarn.RoutingTable;
import ch.epfl.tkvs.yarn.Utils;
import ch.epfl.tkvs.yarn.appmaster.centralized_decision.DeadlockCentralizedDecider;
import ch.epfl.tkvs.yarn.appmaster.centralized_decision.ICentralizedDecider;


public class AppMaster {

    private static Logger log = Logger.getLogger(AppMaster.class.getName());
    private static final int MAX_NUMBER_OF_WORKERS = 10;
    private static RMCallbackHandler rmHandler;
    private static int nextXid = 0;

    public static void main(String[] args) {
        log.info("Initializing at " + NetUtils.getHostname());
        try {
            new AppMaster().run();
        } catch (Exception ex) {
            log.fatal("Failed", ex);
        }
        log.info("Finished");
        System.exit(0);
    }

    public void run() throws Exception {
        YarnConfiguration conf = new YarnConfiguration();

        // Create NM Client
        log.info("Creating NM client");
        NMCallbackHandler nmHandler = new NMCallbackHandler();
        NMClientAsync nmClient = new NMClientAsyncImpl(nmHandler);
        nmClient.init(conf);
        nmClient.start();

        // Create RM Client
        log.info("Creating RM client");
        rmHandler = new RMCallbackHandler(nmClient, conf);
        AMRMClientAsync<ContainerRequest> rmClient = new AMRMClientAsyncImpl<>(1000, rmHandler);
        rmClient.init(conf);
        rmClient.start();

        // Get port and hostname.
        String amIP = Utils.extractIP(NetUtils.getHostname());
        int amPort = NetUtils.getFreeSocketPort();
        rmHandler.setRoutingTable(new RoutingTable(amIP, amPort));

        // Register with RM and create AM Socket
        rmClient.registerApplicationMaster(amIP, amPort, null);
        log.info("Registered and starting server at " + amIP + ":" + amPort);
        ServerSocket server = new ServerSocket(amPort);

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(Utils.TM_MEMORY);
        capability.setVirtualCores(Utils.TM_CORES);

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
        server.setSoTimeout(10000); // 10 seconds accept() timeout.
        try {
            log.info("Waiting for a reply from all TMs");

            // To avoid infinite hang due to pings from the client
            // we keep a counter that indicates us how many subsequent
            // pings occurred.
            int pingCount = 0;

            while (rmHandler.getRoutingTable().size() < tmRequests.size() && pingCount < 3) {
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
                    log.warn("TM at " + tmIp + " did not reply");
                    rmClient.removeContainerRequest(contRequests.get(tmIp));
                }
            }
        }
        server.setSoTimeout(0); // reset timeout.

        log.info("Sending routing information to TMs");
        TMInitMessage initMessage = new TMInitMessage(rmHandler.getRoutingTable());
        
        for (RemoteTransactionManager tm : rmHandler.getRoutingTable().getTMs()) {
        	tm.sendMessage(initMessage, false);
        }

        ICentralizedDecider decider = new DeadlockCentralizedDecider(); // TODO make it configurable
        
        ExecutorService threadPool = Executors.newFixedThreadPool(MAX_NUMBER_OF_WORKERS);
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
    
    public static void sendMessageToTM(Message message, int tmHash) {
    	//TODO implement it
    }
    
    public static int numberOfRegisteredTMs() {
    	return rmHandler.getContainerCount();
    }
    
    public static int nextTransactionId() {
    	int xid = nextXid;
    	nextXid++;
    	return xid;
    }
}