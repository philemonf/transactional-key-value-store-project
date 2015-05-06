package ch.epfl.tkvs.user;

import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;
import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.exceptions.AbortToUserException;
import ch.epfl.tkvs.exceptions.TransactionNotLiveException;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.TransactionManagerRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.TryCommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.TransactionManagerResponse;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;
import ch.epfl.tkvs.yarn.Utils;


public class UserTransaction<K extends Key> {

    public enum TransactionStatus {
        live, aborted, commited
    }

    private String tmHost;
    private int tmPort;
    private int transactionID;
    private TransactionStatus status;
    
    private static InetSocketAddress amAddress = null;
    private static Logger log = Logger.getLogger(UserTransaction.class.getName());

    public UserTransaction(K key) throws AbortException {
        try {
            if (amAddress == null) {
            	amAddress = Utils.readAMAddress();
            }
            
            TransactionManagerRequest req = new TransactionManagerRequest(key.getLocalityHash());

            TransactionManagerResponse response = (TransactionManagerResponse) sendRequest(amAddress.getHostName(), amAddress.getPort(), req, TransactionManagerResponse.class);

            tmHost = response.getHost();
            tmPort = response.getPort();
            transactionID = response.getTransactionId();

            BeginRequest request = new BeginRequest(transactionID);
            GenericSuccessResponse beginResponse = (GenericSuccessResponse) sendRequest(tmHost, tmPort, request, GenericSuccessResponse.class);
            if (!beginResponse.getSuccess()) {
                status = TransactionStatus.aborted;
                throw new AbortToUserException(beginResponse.getExceptionMessage());
            } else {
                status = TransactionStatus.live;
            }

        } catch (Exception e) {
            tmHost = null;
            e.printStackTrace();
        }
    }

    private Message sendRequest(String hostName, int portNumber, Message request, Class<? extends Message> expectedMessageType) {
        try {
            Socket sock = new Socket(hostName, portNumber);

            PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
            out.println(toJSON(request).toString());
            log.info("Sending request to " + hostName + ":" + portNumber);
            log.info(request.toString());

            BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            String inputStr = in.readLine();

            in.close();
            out.close();
            sock.close();
            Message response = parseJSON(new JSONObject(inputStr), expectedMessageType);
            log.info(response + " <-- " + request);
            return response;

        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + hostName);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " + hostName + ":" + portNumber);
            e.printStackTrace();
            System.exit(1);
        } catch (InvalidMessageException | JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Serializable read(K key) throws AbortException {

        if (status != TransactionStatus.live) {
            throw new TransactionNotLiveException();
        }

        ReadRequest request = new ReadRequest(transactionID, key, key.getLocalityHash());

        ReadResponse response = (ReadResponse) sendRequest(tmHost, tmPort, request, ReadResponse.class);

        if (!response.getSuccess()) {
            status = TransactionStatus.aborted;
            throw new AbortToUserException(response.getExceptionMessage());
        }

        return response.getValue();

    }

    public void write(K key, Serializable value) throws AbortException {

        if (status != TransactionStatus.live) {
            throw new TransactionNotLiveException();
        }
        WriteRequest request = new WriteRequest(transactionID, key, value, key.getLocalityHash());
        GenericSuccessResponse response = (GenericSuccessResponse) sendRequest(tmHost, tmPort, request, GenericSuccessResponse.class);

        if (!response.getSuccess()) {
            status = TransactionStatus.aborted;
            throw new AbortToUserException(response.getExceptionMessage());
        }

    }

    public void commit() throws AbortException {

        if (status != TransactionStatus.live) {
            throw new TransactionNotLiveException();
        }
        TryCommitRequest request = new TryCommitRequest(transactionID);
        GenericSuccessResponse response = (GenericSuccessResponse) sendRequest(tmHost, tmPort, request, GenericSuccessResponse.class);

        if (!response.getSuccess()) {
            status = TransactionStatus.aborted;
            throw new AbortToUserException(response.getExceptionMessage());
        }

    }
    
    
    public int getTransactionID() {
        return transactionID;
    }
}
