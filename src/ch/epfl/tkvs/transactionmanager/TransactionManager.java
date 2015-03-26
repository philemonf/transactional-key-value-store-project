package ch.epfl.tkvs.transactionmanager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;

/**
 * The TransactionManager is the deamon started by
 * the {@link AppMaster} on many nodes of the cluster.
 * 
 * It is mainly a server which answers the client requests.
 *
 */
public class TransactionManager {

	private static final int THREAD_POOL_SIZE = 15;
	private static final ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    
	private static Logger log = Logger.getLogger(TransactionManager.class.getName());

    private boolean listening = true;
    private ServerSocket server;
    public static int port = 49200;

    private static KeyValueStore kvStore;

    public static void main(String[] args) {
        try {
            log.info("Initializing...");
            new TransactionManager().run();
        } catch (Exception ex) {
            log.fatal("Could not run transaction manager", ex);
        }
    }

    public void run() throws Exception {
        log.info("Initializing");
        log.info("Host Name: " + InetAddress.getLocalHost().getHostName());

        // Create TM Server
        server = new ServerSocket(port);
        kvStore = new KeyValueStore();

        log.info("Starting server...");
        while (listening) {
            try {
            	
                Socket socket = server.accept();
                Runnable tmThread = new TMThread(socket, kvStore);
            	
                executor.execute(tmThread);
                
            } catch (IOException e) {
                log.error("sock.accept ", e);
            }
        }

        server.close();
        log.info("Finalizing");
    }
}


