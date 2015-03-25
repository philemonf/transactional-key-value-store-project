package ch.epfl.tkvs.transactionmanager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.apache.log4j.Logger;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;


public class TransactionManager {

    private static Logger log = Logger.getLogger(TransactionManager.class.getName());

    private boolean listening = true;
    private ServerSocket sock;
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

        // Create TM Socket
        sock = new ServerSocket(port);
        kvStore = new KeyValueStore();

        log.info("Starting server...");
        while (listening) {
            try {
                new TMThread(sock.accept(), kvStore).start();
            } catch (IOException e) {
                log.error("sock.accept ", e);
            }
        }

        sock.close();
        log.info("Finalizing");
    }
}
