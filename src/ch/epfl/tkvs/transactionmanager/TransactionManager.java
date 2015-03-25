package ch.epfl.tkvs.transactionmanager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.apache.log4j.Logger;

import ch.epfl.tkvs.kvstore.KeyValueStore;


public class TransactionManager {

    private static Logger log = Logger.getLogger(TransactionManager.class.getName());

    public static int port = 28404;
    private static boolean listening = true;

    private static KeyValueStore kvStore;

    public static void main(String[] args) throws Exception {
        log.info("Initializing");
        log.info("Host Name: " + InetAddress.getLocalHost().getHostName());
        kvStore = new KeyValueStore();
        try {
            ServerSocket sock = new ServerSocket(port);
            while (listening) {
                new TMThread(sock.accept(), kvStore, log).start();
            }
            sock.close();
        } catch (IOException e) {
            log.fatal("Could not listen on port " + port, e);
            System.exit(-1);

        }
        log.info("Finalizing");
    }
}
