package ch.epfl.tkvs.transactionmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import ch.epfl.tkvs.config.SlavesConfig;
import ch.epfl.tkvs.keyvaluestore.KeyValueStore;
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

    private static boolean listening = true;
    private ServerSocket server;
    private String hostname;

    private static KeyValueStore kvStore;

    public static void main(String[] args) {
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
        // TODO: Fix this when tested on cluster!
        // this.hostname = InetAddress.getLocalHost().getCanonicalHostName();
        this.hostname = "localhost";
    }

    public void run() throws Exception {
        log.info("Host Name: " + hostname);

        SlavesConfig slaveConfig = new SlavesConfig();
        // Create TM Server
        server = new ServerSocket(slaveConfig.getPortForHost(hostname));
        kvStore = new KeyValueStore();

        log.info("Starting server...");
        while (listening) {
            try {
                Socket sock = server.accept();

                BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                String input = in.readLine();

                switch (input) {
                case ":exit":
                    log.info("Stopping Server");
                    listening = false;
                    sock.close();
                    server.close();
                    break;
                default:
                    threadPool.execute(new TMWorker(input, sock, kvStore));
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
