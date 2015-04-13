package ch.epfl.tkvs.user;

import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;
import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.config.SlavesConfig;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.TransactionManagerRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.TransactionManagerResponse;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;


public class Transaction<K extends Key> {

    public enum TransactionStatus {
        live, aborted, commited
    }

    private static String amHost;
    private static int amPort;
    private String tmHost;
    private int tmPort;
    private int transactionID;
    private TransactionStatus status;

    public Transaction(K key) {
        try {
            // TODO: Find how to deal with that.
            amHost = "localhost";
            amPort = SlavesConfig.AM_DEFAULT_PORT;

            TransactionManagerRequest req = new TransactionManagerRequest(key.getHash());

            JSONObject jsonResponse = sendRequest(amHost, amPort, toJSON(req));
            TransactionManagerResponse response = (TransactionManagerResponse) parseJSON(jsonResponse,
                    TransactionManagerResponse.class);

            tmHost = response.getHost();
            tmPort = response.getPort();
            transactionID = response.getTransactionId();
            status = TransactionStatus.live;

        } catch (JSONException | InvalidMessageException e) {
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

    public Serializable read(K key) throws Exception {

        if (status != TransactionStatus.live) {
            throw new AbortException("Transaction is no longer live");
        }

        ReadRequest request = new ReadRequest(transactionID, key, key.getHash());

        JSONObject json = sendRequest(tmHost, tmPort, toJSON(request));
        ReadResponse response = (ReadResponse) parseJSON(json, ReadResponse.class);

        if (!response.getSuccess()) {
            status = TransactionStatus.aborted;
            throw new AbortException("Abort");
        }

        return response.getValue();
    }

    public void write(K key, Serializable value) throws Exception {

        if (status != TransactionStatus.live) {
            throw new AbortException("Transaction is no longer live");
        }
        WriteRequest request = new WriteRequest(transactionID, key, value, key.getHash());
        JSONObject response = sendRequest(tmHost, tmPort, toJSON(request));

        boolean isSuccess = response.getBoolean(JSONCommunication.KEY_FOR_SUCCESS);
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
        request.put("request_type", "Commit");
        request.put("transaction_id", transactionID);
        JSONObject response = sendRequest(tmHost, tmPort, request);
        boolean isSuccess = response.getBoolean("Success");
        if (!isSuccess) {
            status = TransactionStatus.aborted;
            throw new AbortException("Abort");
        }
    }

}
