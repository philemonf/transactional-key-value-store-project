package ch.epfl.tkvs.transactionmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import ch.epfl.tkvs.config.NetConfig;
import ch.epfl.tkvs.transactionmanager.algorithms.Algorithm;
import ch.epfl.tkvs.transactionmanager.algorithms.MVCC2PL;
import ch.epfl.tkvs.yarn.appmaster.AppMaster;


/**
 * The TransactionManager is the deamon started by the {@link AppMaster} on many nodes of the cluster.
 * 
 * It is mainly a server which answers the client requests.
 *
 */
public class TransactionManager {

    private static final int THREAD_POOL_SIZE = 15;
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    private static Logger log = Logger.getLogger(TransactionManager.class.getName());

    private ServerSocket server;
    private String hostname;

    public static void main(String[] args) throws Exception {
    	
        log.info("Initializing...");
        try {
            new TransactionManager().run();
        } catch (Exception ex) {
            log.fatal("Could not run transaction manager", ex);
        }
        log.info("Finished");
        System.exit(0);
    }

    public TransactionManager() throws UnknownHostException {
        this.hostname = InetAddress.getLocalHost().getHostName();
    }

    public void run() throws Exception {
        log.info("Host Name: " + hostname);

        NetConfig slaveConfig = new NetConfig();
        
        // Create TM Server
        int port = slaveConfig.getPortForHost(hostname);
        server = new ServerSocket(port);

        Algorithm concurrencyController = new MVCC2PL();

        log.info("Starting server at " + hostname + ":" + port);
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
        // TODO: write KV store to disk ?
    }
}
