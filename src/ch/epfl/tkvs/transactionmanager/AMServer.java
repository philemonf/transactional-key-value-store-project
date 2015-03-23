package ch.epfl.tkvs.transactionmanager;

import java.io.IOException;
import java.net.ServerSocket;


public class AMServer implements Runnable {

    public static int portNumber = 24744;

    public static void main(String[] args) throws IOException {
        new AMServer().run();
    }

    @Override
    public void run() {
        System.out.println("AM Server starting...");

        boolean listening = true;

        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
            while (listening) {
                new AMThread(serverSocket.accept()).start();
                // listening = false;
            }
        } catch (IOException e) {
            System.err.println("Could not listen on port " + portNumber);
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
