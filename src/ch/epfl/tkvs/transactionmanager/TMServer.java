package ch.epfl.tkvs.transactionmanager;

import java.io.IOException;
import java.net.ServerSocket;

public class TMServer {
    static int portNumber = 1070;
	public static void main(String[] args) throws IOException {

		System.out.println("TM Server starting...");
		boolean listening = true;

		try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
			while (listening) {
				new TMThread(serverSocket.accept(), serverSocket.getLocalPort()).start();
			}
		} catch (IOException e) {
			System.err.println("Could not listen on port " + portNumber);
			System.exit(-1);
		}
	}
}