package ch.epfl.tkvs.transactionmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.kvstore.KeyValueStore;


public class TMThread extends Thread {

    private Socket socket;
    private byte[] valueRead;
    private KeyValueStore kvStore;
    private Logger log;

    public TMThread(Socket socket, KeyValueStore kvStore, Logger log) {
        this.socket = socket;
        this.kvStore = kvStore;
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
            case "Read":
                response = jsonifyReadRequest(request);
                break;
            case "Write":
                response = jsonifyWriteRequest(request);
                break;
            case "Commit":
                response = jsonifyCommitRequest(request);
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

    private JSONObject jsonifyCommitRequest(JSONObject request) throws JSONException {
        long transactionID = request.getLong("TransactionID");
        boolean success = commit(transactionID);

        JSONObject response = new JSONObject();
        response.put("Success", success);
        return response;
    }

    private boolean commit(long transactionID) {
        return true;
    }

    private JSONObject jsonifyWriteRequest(JSONObject request) throws JSONException {
        long transactionID = request.getLong("TransactionID");
        String key = request.getString("Key");
        int hash = request.getInt("Hash");
        byte[] value = request.getString("Value").getBytes();
        boolean success = write(transactionID, key, value);

        JSONObject response = new JSONObject();
        response.put("Success", success);
        return response;
    }

    private boolean write(long transactionID, String key, byte[] value) {
        log.info("Write  " + key + "   " + value);
        kvStore.put(key, value);
        return true;
    }

    private JSONObject jsonifyReadRequest(JSONObject request) throws JSONException {
        long transactionID = request.getLong("TransactionID");
        String key = request.getString("Key");
        int hash = request.getInt("Hash");
        boolean success = read(transactionID, key);

        JSONObject response = new JSONObject();
        response.put("Success", success);
        response.put("Value", new String(valueRead));
        valueRead = null;
        return response;
    }

    private boolean read(long transactionID, String key) {
        // Updates valueRead
        valueRead = kvStore.get(key);

        log.info("Read " + key + "   " + valueRead);
        return true;
    }

}
