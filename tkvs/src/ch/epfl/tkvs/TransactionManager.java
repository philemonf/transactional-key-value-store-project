package ch.epfl.tkvs;

import java.net.InetAddress;

public class TransactionManager {
	public static void main(String[] args) throws Exception {
		System.out.println("TKVS TransactionManager: Initializing");
		System.out.println("BONJOUR!");
		System.out.println(InetAddress.getLocalHost().getHostAddress());
		System.out.println(InetAddress.getLocalHost().getHostName());
		System.out.println("TKVS TransactionManager: Finalizing");
	}
}
