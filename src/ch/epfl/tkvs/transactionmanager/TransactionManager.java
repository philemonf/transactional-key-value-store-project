package ch.epfl.tkvs.transactionmanager;

import ch.epfl.tkvs.kvstore.Key;
import ch.epfl.tkvs.kvstore.Value;
import ch.epfl.tkvs.lockingunit.LockType;
import ch.epfl.tkvs.lockingunit.LockingUnit;
import static ch.epfl.tkvs.transactionmanager.TransactionManager.portNumber;
import ch.epfl.tkvs.versioningunit.VersioningUnit;
import java.io.IOException;

import java.net.InetAddress;
import java.net.ServerSocket;

public class TransactionManager
  {

    static int portNumber = 1070;

    public static void main(String[] args) throws Exception
      {
        System.out.println("TKVS TransactionManager: Initializing");
        System.out.println("TKVS TransactionManager: Host Address: " + InetAddress.getLocalHost().getHostAddress());
        System.out.println("TKVS TransactionManager: Host Name: " + InetAddress.getLocalHost().getHostName());
        System.out.println("TKVS TransactionManager: Finalizing");
        boolean listening = false;
        // Dummy examples
        LockingUnit.instance.lock(new Key(), LockType.READ_LOCK);
        LockingUnit.instance.release(new Key(), LockType.WRITE_LOCK);
        VersioningUnit.instance.createNewVersion(new Key(), new Value());
        try (ServerSocket serverSocket = new ServerSocket(portNumber))
          {
            while (listening)
              {
                new TMThread(serverSocket.accept(), serverSocket.getLocalPort()).start();
//		listening=false;
              }
          } catch (IOException e)
          {
            System.err.println("Could not listen on port " + portNumber);
            System.exit(-1);

          }
      }
  }
