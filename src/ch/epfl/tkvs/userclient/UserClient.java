package ch.epfl.tkvs.userclient;

import ch.epfl.tkvs.transactionmanager.AMServer;

public class UserClient {

	public static void main(String[] args) throws Exception {
		System.out.println("Client starting");
		Transaction.initialize("localhost", AMServer.portNumber);
		MyKey k0 = new MyKey(0);
		Transaction t = new Transaction(k0);
		t.write(k0, new MyValue("zero"));
		System.out.println(t.read(k0));

	}
}
