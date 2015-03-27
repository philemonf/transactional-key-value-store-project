package ch.epfl.tkvs.test.userclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class Transaction<K extends Key> {

    public enum TransactionStatus {
        live, aborted, commited
    }

    private static String amHost;
    private static int amPort;
    private String tmHost;
    private int tmPort;
    private long transactionID;
    private TransactionStatus status;

    public static void initialize(String host, int port) {
        amHost = host;
        amPort = port;
    }

    public Transaction(K key) {
        try {
            JSONObject request = new JSONObject();
            request.put("Type", "TM");
            request.put("Hash", key.getHash());
            request.put("Key", key);
            JSONObject response = sendRequest(amHost, amPort, request);
            tmHost = response.getString("HostName");
            tmPort = response.getInt("PortNumber");
            transactionID = response.getLong("TransactionID");
            status = TransactionStatus.live;
        } catch (JSONException e) {
            tmHost = null;
            e.printStackTrace();
        }
    }

    private JSONObject sendRequest(String hostName, int portNumber, JSONObject request) {
        try {
            Socket sock = new Socket(hostName, portNumber);

            PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
            out.println(request.toString());
            System.out.println("Sending request to " + hostName + ":" + portNumber);
            System.out.println(request.toString());

            BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            String inputStr = in.readLine();
            System.out.println("Received response from " + hostName + ":" + portNumber);
            System.out.println(inputStr);

            in.close();
            out.close();
            sock.close();
            return new JSONObject(inputStr);

        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + hostName);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " + hostName + ":" + portNumber);
            e.printStackTrace();
            System.exit(1);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    public byte[] read(K key) throws AbortException, JSONException {

        if (status != TransactionStatus.live) {
            throw new AbortException("Transaction is no longer live");
        }
        JSONObject request = new JSONObject();

        request.put("Type", "Read");
        request.put("TransactionID", transactionID);
        request.put("Hash", key.getHash());
        request.put("Key", key.toString());
        JSONObject response = sendRequest(tmHost, tmPort, request);

        boolean isSuccess = response.getBoolean("Success");
        if (!isSuccess) {
            status = TransactionStatus.aborted;
            throw new AbortException("Abort");
        }
        return response.getString("Value").getBytes();
    }

    public void write(K key, byte[] value) throws AbortException, JSONException {

        if (status != TransactionStatus.live) {
            throw new AbortException("Transaction is no longer live");
        }
        JSONObject request = new JSONObject();
        request.put("Type", "Write");
        request.put("TransactionID", transactionID);
        request.put("Hash", key.getHash());
        request.put("Key", key.toString());
        request.put("Value", new String(value));
        JSONObject response = sendRequest(tmHost, tmPort, request);
        boolean isSuccess = response.getBoolean("Success");
        if (!isSuccess) {
            status = TransactionStatus.aborted;
            throw new AbortException("Abort");
        }
    }

    private void commit() throws AbortException, JSONException {

        if (status != TransactionStatus.live) {
            throw new AbortException("Transaction is no longer live");
        }
        JSONObject request = new JSONObject();
        request.put("Type", "Commit");
        request.put("TransactionID", transactionID);
        JSONObject response = sendRequest(tmHost, tmPort, request);
        boolean isSuccess = response.getBoolean("Success");
        if (!isSuccess) {
            status = TransactionStatus.aborted;
            throw new AbortException("Abort");
        }
    }

}
