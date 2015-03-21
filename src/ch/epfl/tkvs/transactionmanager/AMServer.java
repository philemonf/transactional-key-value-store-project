package ch.epfl.tkvs.transactionmanager;

import java.io.IOException;
import java.net.ServerSocket;

public class AMServer {
   public static int portNumber = 1090;
	public static void main(String[] args) throws IOException {
System.out.println("AM Server starting...");
		
		boolean listening = true;

		try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
			while (listening) {
				new AMThread(serverSocket.accept()).start();
			}
		} catch (IOException e) {
			System.err.println("Could not listen on port " + portNumber);
			System.exit(-1);
		}
	}
}