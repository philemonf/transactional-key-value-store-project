package ch.epfl.tkvs.yarn.appmaster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.TransactionManager;


public class AMThread extends Thread {

    private Socket socket;
    private Logger log;

    public AMThread(Socket socket, Logger log) {
        this.socket = socket;
        this.log = log;
    }

    public void run() {
        try {
            // Read the request into a JSONObject
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String inputStr = in.readLine();
            
            // Create the response
            JSONObject request = new JSONObject(inputStr);
            JSONObject response = null;
            switch (request.getString("Type")) {
            case "TM":
                response = jsonifyTMRequest(request);
                break;
            }

            // Send the response
            log.info("Response" + response.toString());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(response.toString());

            in.close();
            out.close();
            socket.close();
        } catch (IOException | JSONException e) {
            e.printStackTrace();
        }
    }

    private JSONObject jsonifyTMRequest(JSONObject request) throws JSONException {
        String key = request.getString("Key");
        int hash = request.getInt("Hash");

        // Compute the hash of the key.
        String hostName = "localhost";
        int portNumber = TransactionManager.port;
        long transactionID = 0;

        // get the hostName and portNumber for that hash.
        // create a unique transactionID
        log.info("Begin " + transactionID);
        JSONObject response = new JSONObject();
        response.put("HostName", hostName);
        response.put("PortNumber", portNumber);
        response.put("TransactionID", transactionID);
        return response;
    }
}