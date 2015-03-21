package ch.epfl.tkvs.transactionmanager;

import ch.epfl.tkvs.kvstore.Key;
import ch.epfl.tkvs.kvstore.Value;
import ch.epfl.tkvs.lockingunit.LockType;
import ch.epfl.tkvs.lockingunit.LockingUnit;
import ch.epfl.tkvs.versioningunit.VersioningUnit;

import java.net.InetAddress;

public class TransactionManager {
	public static void main(String[] args) throws Exception {
		System.out.println("TKVS TransactionManager: Initializing");
		System.out.println("TKVS TransactionManager: Host Address: " + InetAddress.getLocalHost().getHostAddress());
		System.out.println("TKVS TransactionManager: Host Name: " + InetAddress.getLocalHost().getHostName());
		System.out.println("TKVS TransactionManager: Finalizing");
		
		// Dummy examples
		LockingUnit.instance.lock(new Key(), LockType.READ_LOCK);
        LockingUnit.instance.release(new Key(), LockType.WRITE_LOCK);
        VersioningUnit.instance.createNewVersion(new Key(), new Value());
    }
}
