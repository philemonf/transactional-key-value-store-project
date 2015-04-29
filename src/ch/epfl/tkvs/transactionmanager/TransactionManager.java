package ch.epfl.tkvs.transactionmanager;

import ch.epfl.tkvs.config.NetConfig;
import ch.epfl.tkvs.transactionmanager.algorithms.Algorithm;
import ch.epfl.tkvs.transactionmanager.algorithms.MVCC2PL;
import ch.epfl.tkvs.yarn.appmaster.AppMaster;
import org.apache.hadoop.net.NetUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


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

    private static Logger log = Logger.getLogger(TransactionManager.class.getName());

    public static void main(String[] args) throws Exception {

        log.info("Initializing...");
        try {
            String amHost = args[0];
            int amPort = Integer.parseInt(args[1]);
            tmPort = NetUtils.getFreeSocketPort();
            tmHost = NetConfig.getOnlyHostname(NetUtils.getHostname());

            log.info("Sending Host:Port to AppMaster");
            Socket sock = new Socket(amHost, amPort);
            PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
            out.println("registertm:" + tmHost + ":" + tmPort);
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

        Algorithm concurrencyController = new MVCC2PL();
        ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        while (!server.isClosed()) {
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
