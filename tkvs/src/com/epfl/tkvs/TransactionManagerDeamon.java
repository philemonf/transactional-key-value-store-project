package com.epfl.tkvs;

import java.net.ServerSocket;
import java.net.Socket;

public class TransactionManagerDeamon {
	public static void main(String[] args) throws Exception {
		ServerSocket server = new ServerSocket(9999);
		
		while (true) {
			Socket socket = server.accept();
			socket.getOutputStream().write("Hello!".getBytes());
			socket.getOutputStream().flush();
		}
	}
}
