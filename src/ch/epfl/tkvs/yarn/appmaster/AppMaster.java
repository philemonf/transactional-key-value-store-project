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

import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
import ch.epfl.tkvs.yarn.RoutingTable;
import ch.epfl.tkvs.yarn.Utils;


public class AppMaster {

    private static Logger log = Logger.getLogger(AppMaster.class.getName());
    private static final int MAX_NUMBER_OF_WORKERS = 10;

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
        RMCallbackHandler rmHandler = new RMCallbackHandler(nmClient, conf);
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
            while (rmHandler.getRoutingTable().size() < tmRequests.size()) {
                Socket sock = server.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                String input = in.readLine();
                if (input.equals(":ping")) {
                	log.info("Receive a premature ping from the client.");
                	PrintWriter out = new PrintWriter(sock.getOutputStream());
                	out.println("not ready");
                	out.flush();
                	out.close();
                } else {
                	String[] info = input.split(":");
                	log.info("Registering TM at " + info[0] + ":" + info[1]);
                	rmHandler.getRoutingTable().addTM(info[0], Integer.parseInt(info[1]));
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
        String routingEncoded = Base64Utils.convertToBase64(rmHandler.getRoutingTable());
        for (Entry<String, Integer> tm : rmHandler.getRoutingTable().getTMs().entrySet()) {
            Socket sock = new Socket(tm.getKey(), tm.getValue());
            PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
            out.print(routingEncoded);
            out.close();
            sock.close();
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(MAX_NUMBER_OF_WORKERS);
        while (!server.isClosed() && rmHandler.getContainerCount() > 0) {
            try {
                log.info("Waiting for message...");
                Socket sock = server.accept();
                log.info("Processing message...");

                BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                String input = in.readLine();

                switch (input) {
                case ":ping":
                {
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
                    for (Entry<String, Integer> ent : rmHandler.getRoutingTable().getTMs().entrySet()) {
                        Socket exitSock = new Socket(ent.getKey(), ent.getValue());
                        PrintWriter out = new PrintWriter(exitSock.getOutputStream(), true);
                        out.println(input);
                        out.close();
                        exitSock.close();
                    }
                    break;
                default:
                    try {
                        JSONObject jsonRequest = new JSONObject(input);
                        threadPool.execute(new AMWorker(rmHandler.getRoutingTable(), jsonRequest, sock));
                    } catch (JSONException e) {
                        log.warn("Non JSON message will not be parsed: " + input);
                    }
                }

            } catch (IOException e) {
                log.error("sock.accept ", e);
            }
        }

        while (rmHandler.getContainerCount() > 0) {
            Thread.sleep(1000);
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
}