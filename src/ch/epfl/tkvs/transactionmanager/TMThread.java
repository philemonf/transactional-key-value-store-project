package ch.epfl.tkvs.transactionmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.kvstore.KeyValueStore;


public class TMThread extends Thread {

    private Socket socket = null;
    private byte[] valueRead = null;
    private KeyValueStore kvStore = null;
    private int portNumber;

    public TMThread(Socket socket, int portNumber, KeyValueStore kvStore) {
        super("TMServerThread");
        this.socket = socket;
        this.portNumber = portNumber;
        this.kvStore = kvStore;
    }

    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);) {

            // Read the request into a JSONObject

            String inputStr;
            inputStr = in.readLine();

            JSONObject request = new JSONObject(inputStr);

            // Create the response
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
            System.out.println("Response" + response.toString());
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
        System.out.println("Write  " + key + "   " + value);
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

        System.out.println("Read " + key + "   " + valueRead);
        return true;
    }

}
