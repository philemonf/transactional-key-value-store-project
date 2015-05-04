package ch.epfl.tkvs.transactionmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.net.NetUtils;
import org.apache.log4j.Logger;

import ch.epfl.tkvs.transactionmanager.algorithms.Algorithm;
import ch.epfl.tkvs.transactionmanager.algorithms.MVTO;
import ch.epfl.tkvs.transactionmanager.algorithms.Simple2PL;
import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
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

    private static final int THREAD_POOL_SIZE = 15;
    private static String tmHost;
    private static int tmPort;
    private static RoutingTable routing;

    private static Logger log = Logger.getLogger(TransactionManager.class.getName());

    public static void main(String[] args) throws Exception {

        log.info("Initializing...");
        try {
            String amIp = System.getenv("AM_IP");
            int amPort = Integer.parseInt(System.getenv("AM_PORT"));
            tmPort = NetUtils.getFreeSocketPort();
            tmHost = Utils.extractIP(NetUtils.getHostname());

            log.info("Sending Host:Port to AppMaster");
            Socket sock = new Socket(amIp, amPort);
            PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
            out.println(tmHost + ":" + tmPort);
            out.close();
            sock.close();

            new TransactionManager().run();
        } catch (Exception ex) {
            log.fatal("Could not run transaction manager", ex);
        }
        log.info("Finished");
        System.exit(0);
    }

    public void run() throws Exception {
        log.info("Starting server at " + tmHost + ":" + tmPort);
        ServerSocket server = new ServerSocket(tmPort);

        Socket sock = server.accept();
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String input = in.readLine();
        routing = (RoutingTable) Base64Utils.convertFromBase64(input);

        Algorithm concurrencyController = new Simple2PL(null);
        ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        while (!server.isClosed()) {
            try {
                log.info("Waiting for message...");
                sock = server.accept();
                log.info("Processing message...");

                in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                input = in.readLine();

                switch (input) {
                case ":exit":
                    log.info("Stopping Server");
                    sock.close();
                    server.close();
                    threadPool.shutdown();
                    break;
                default:
                    threadPool.execute(new TMWorker(input, sock, concurrencyController));
                }
            } catch (IOException e) {
                log.error("sock.accept ", e);
            }
        }
        log.info("Finalizing");
        server.close();
    }
}
