package ch.epfl.tkvs.transactionmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.net.NetUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.algorithms.CCAlgorithm;
import ch.epfl.tkvs.transactionmanager.algorithms.MVTO;
import ch.epfl.tkvs.transactionmanager.algorithms.RemoteHandler;
import ch.epfl.tkvs.transactionmanager.communication.ExitMessage;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.TMInitMessage;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter;
import ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter;
import ch.epfl.tkvs.yarn.HDFSLogger;
import ch.epfl.tkvs.yarn.RemoteTransactionManager;
import ch.epfl.tkvs.yarn.RoutingTable;
import ch.epfl.tkvs.yarn.Utils;
import ch.epfl.tkvs.yarn.appmaster.AppMaster;


/**
 * The TransactionManager is the deamon started by the {@link AppMaster} on many nodes of the cluster.
 * 
 * It is mainly a server which answers the client requests.
 *
 */
public class TransactionManager {

    private static final int CHECKPOINT_PERIOD_MS = 15000;
    private static String tmIp;
    private static int tmPort;
    private static RoutingTable routing;
    private static boolean isAMReady = false;

    private final static HDFSLogger log = new HDFSLogger(TransactionManager.class);

    public static void main(String[] args) throws Exception {
        Utils.initLogLevel();
        log.info("Initializing..." + args[0], TransactionManager.class);
        try {
            String amIp = System.getenv("AM_IP");
            int amPort = Integer.parseInt(System.getenv("AM_PORT"));

            tmPort = NetUtils.getFreeSocketPort();
            tmIp = Utils.extractIP(NetUtils.getHostname());

            log.info("Sending ip:port to AppMaster", TransactionManager.class);
            Socket sock = new Socket(amIp, amPort);
            PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
            out.println(tmIp + ":" + tmPort);
            out.close();
            sock.close();

            new TransactionManager().run();
        } catch (Exception ex) {
            log.fatal("Could not run transaction manager", ex, TransactionManager.class);
        }
        log.info("Finished", TransactionManager.class);
        log.writeToHDFS(args[0]);
        System.exit(0);
    }

    public void run() throws Exception {
        log.info("Starting server at " + tmIp + ":" + tmPort, TransactionManager.class);
        ServerSocket server = new ServerSocket(tmPort);

        // Wait for the TM initialization message
        Socket sock = server.accept();
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String input = in.readLine();
        TMInitMessage initMessage = (TMInitMessage) JSON2MessageConverter.parseJSON(new JSONObject(input), TMInitMessage.class);
        routing = initMessage.getRoutingTable();

        RemoteHandler remoteHandler = new RemoteHandler();

        // Select which concurrency algorithm to use
        CCAlgorithm concurrencyController = new MVTO(remoteHandler, log);

        remoteHandler.setAlgo(concurrencyController, log);

        // Start the thread that will call checkpoint on the concurrency controller
        startCheckpointThread(server, concurrencyController);

        ExecutorService threadPool = Executors.newCachedThreadPool();

        while (!server.isClosed()) {
            try {
                // log.info("Waiting for message...", TransactionManager.class);
                sock = server.accept();
                // log.info("Processing message...", TransactionManager.class);

                in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                input = in.readLine();

                JSONObject json = new JSONObject(input);
                String messageType = json.getString(JSONCommunication.KEY_FOR_MESSAGE_TYPE);

                if (messageType.equals(ExitMessage.MESSAGE_TYPE)) {
                    log.info("Stopping Server", TransactionManager.class);
                    sock.close();
                    server.close();
                    threadPool.shutdown();
                } else {
                    threadPool.execute(new TMWorker(new JSONObject(input), sock, concurrencyController, log));
                }

            } catch (IOException e) {
                log.error("sock.accept ", e, TransactionManager.class);
            }
        }
        log.info("Finalizing", TransactionManager.class);
        server.close();
    }

    /**
     * Helper method to send a message to the app master. Might be blocking if the app master is not ready on start up.
     * @param message the message to send
     * @param shouldWait whether or not one want to get an answer
     * @return the answer if it shouldWait or null
     * @throws IOException in case of network error or invalid message format
     */
    public static JSONObject sendToAppMaster(Message message, boolean shouldWait) throws IOException {

        Socket sock = new Socket(routing.getAMIp(), routing.getAMPort());
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        PrintWriter out = new PrintWriter(sock.getOutputStream());

        while (!isAMReady) {
            out.println(":ping");
            out.flush();

            String response = in.readLine();
            if (response != null && response.equals("ok")) {
                isAMReady = true;
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }

        JSONObject json = null;
        try {
            json = Message2JSONConverter.toJSON(message);
        } catch (JSONException e) {
            log.error("Error", e, TransactionManager.class);
            throw new IOException("Error while converting the message: " + e);
        }

        out.println(json.toString());
        out.flush();

        JSONObject res = null;
        if (shouldWait) {
            String input = in.readLine();
            try {
                res = new JSONObject(input);
            } catch (JSONException e) {
                throw new IOException("Error while building response: " + e);
            }
        }

        in.close();
        out.close();
        sock.close();

        return res;
    }

    /**
     * Send a message to a transaction manager identified by its locality hash.
     * @param localityHash the locality hash of the TM that will receive the message
     * @param message the message to send
     * @param shouldWait whether or not the method should wait for a response
     * @return the response or null if !shouldWait
     * @throws IOException in case of network failure or invalid message
     */
    public static JSONObject sendToTransactionManager(int localityHash, Message message, boolean shouldWait) throws IOException {
        log.info("Sending " + message + "to " + routing.findTM(localityHash), RemoteHandler.class);
        return routing.findTM(localityHash).sendMessage(message, shouldWait);
    }

    /**
     * Returns a list of others TMs.
     * @return a list of RemoteTransactionManager object
     */
    public static List<RemoteTransactionManager> getOtherTMs() {
        List<RemoteTransactionManager> tms = new LinkedList<RemoteTransactionManager>();

        for (RemoteTransactionManager tm : routing.getTMs()) {
            if (!tm.getIp().equals(tmIp) || tm.getPort() != tmPort) {
                tms.add(tm);
            }
        }

        return tms;
    }

    /**
     * Returns the locality hash associated with the passed RemoteTransactionManager.
     * @param a remote transaction manager object
     * @return the locality hash or -1 in case of error
     */
    public static int getLocalityHash(RemoteTransactionManager tm) {

        if (tm == null)
            return -1;

        int hash = 0;
        for (RemoteTransactionManager anotherTM : routing.getTMs()) {

            if (anotherTM.getIp().equals(tm.getIp()) && anotherTM.getPort() == tm.getPort()) {
                return hash;
            }

            ++hash;
        }

        return -1;
    }

    /**
     * Returns the locality hash associated with the local node.
     * @return the locality hash or -1 in case of error
     */
    public static int getLocalityHash() {
        int i = 0;
        for (RemoteTransactionManager tm : routing.getTMs()) {
            if (tm.getIp().equals(tmIp) && tm.getPort() == tmPort) {
                return i;
            }
            ++i;
        }

        return -1;
    }

    /**
     * Returns the number of transaction managers.
     * @return the number of transaction managers
     */
    public static int getNumberOfTMs() {
        return routing.size();
    }

    public static boolean isLocalLocalityHash(int localityHash) {
        return (localityHash % routing.getTMs().size()) == getLocalityHash();
    }

    // Start the thread responsible for calling the checkpoint methods of the concurrency control algorithms
    private void startCheckpointThread(final ServerSocket mainServer, final CCAlgorithm ccAlg) {
        new Thread(new Runnable() {

            @Override
            public void run() {
                while (!mainServer.isClosed()) {

                    try {
                        Thread.sleep(CHECKPOINT_PERIOD_MS);
                    } catch (InterruptedException e) {
                        // TODO Handle this error
                        e.printStackTrace();
                    }

                    ccAlg.checkpoint();
                }
            }
        }).start();
    }
}
