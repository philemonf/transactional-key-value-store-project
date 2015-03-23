package ch.epfl.tkvs.transactionmanager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import ch.epfl.tkvs.kvstore.KeyValueStore;
import ch.epfl.tkvs.lockingunit.LockType;
import ch.epfl.tkvs.lockingunit.LockingUnit;
import ch.epfl.tkvs.userclient.Key;
import ch.epfl.tkvs.userclient.Value;
import ch.epfl.tkvs.versioningunit.VersioningUnit;


public class TransactionManager {

    static int portNumber = 28404;

    private static KeyValueStore kvStore;
    
    public static void main(String[] args) throws Exception {
        System.out.println("TKVS TransactionManager: Initializing");
        System.out.println("TKVS TransactionManager: Host Address: " + InetAddress.getLocalHost().getHostAddress());
        System.out.println("TKVS TransactionManager: Host Name: " + InetAddress.getLocalHost().getHostName());
        System.out.println("TKVS TransactionManager: Finalizing");
        boolean listening = true;
        

        kvStore = new KeyValueStore();
        
        // Dummy examples
//        LockingUnit.instance.lock(new Key(), LockType.READ_LOCK);
//        LockingUnit.instance.release(new Key(), LockType.WRITE_LOCK);
//        VersioningUnit.instance.createNewVersion(new Key(), new Value());
        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
            while (listening) {
                new TMThread(serverSocket.accept(), serverSocket.getLocalPort(), kvStore).start();
                // listening=false;
            }
        } catch (IOException e) {
            System.err.println("Could not listen on port " + portNumber);
            e.printStackTrace();
            System.exit(-1);

        }
    }
}
