package ch.epfl.tkvs.transactionmanager;

import java.net.InetAddress;

public class TransactionManager {
	public static void main(String[] args) throws Exception {
		System.out.println("TKVS TransactionManager: Initializing");
		System.out.println("TKVS TransactionManager: Host Address: " + InetAddress.getLocalHost().getHostAddress());
		System.out.println("TKVS TransactionManager: Host Name: " + InetAddress.getLocalHost().getHostName());
		System.out.println("TKVS TransactionManager: Finalizing");
		
		// Dummy examples
		LockingUnit.instance.lock(new Key(), READ_LOCK);
		LockingUnit.instance.release(new Key(), WRITE_LOCK);
	}
}
