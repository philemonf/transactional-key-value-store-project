package ch.epfl.tkvs.user;

import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;
import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.exceptions.AbortToUserException;
import ch.epfl.tkvs.exceptions.RemoteTMException;
import ch.epfl.tkvs.exceptions.TransactionNotLiveException;
import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.TransactionManagerRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.TryCommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.TransactionManagerResponse;
import ch.epfl.tkvs.yarn.HDFSLogger;
import ch.epfl.tkvs.yarn.Utils;
import ch.epfl.tkvs.yarn.appmaster.AppMaster;


public class UserTransaction<K extends Key> {

    public enum TransactionStatus {
        uninitialized, live, aborted, commited
    }

    private String tmIp; // IP address of primary transaction manager
    private int tmPort; // port number of primary transaction manager
    private int transactionID;
    private TransactionStatus status;

    private static InetSocketAddress amAddress = null;
    public static HDFSLogger log = new HDFSLogger(UserTransaction.class);

    /**
     * method to initialize a transaction at the user client side.
     * @param key The key which hints the {@link AppMaster} to decide which {@link TransactionManager} should be
     * designated as primary for this transaction
     * @throws AbortException if operation was unsuccessful
     */
    public void begin(K key) throws AbortException {
        try {
            if (amAddress == null) {
                amAddress = Utils.readAMAddress();
            }

            TransactionManagerRequest req = new TransactionManagerRequest(key.getLocalityHash());

            TransactionManagerResponse response = (TransactionManagerResponse) sendRequest(amAddress.getHostName(), amAddress.getPort(), req, TransactionManagerResponse.class);

            tmIp = response.getIp();
            tmPort = response.getPort();
            transactionID = response.getTransactionId();

            BeginRequest request = new BeginRequest(transactionID);
            GenericSuccessResponse beginResponse = (GenericSuccessResponse) sendRequest(tmIp, tmPort, request, GenericSuccessResponse.class);
            if (!beginResponse.getSuccess()) {
                status = TransactionStatus.aborted;
                throw new AbortToUserException(beginResponse.getExceptionMessage());
            } else {
                status = TransactionStatus.live;
            }

        } catch (Exception e) {
            tmIp = null;
            e.printStackTrace();
        }
    }

    private Message sendRequest(String ip, int port, Message request, Class<? extends Message> expectedMessageType) throws Exception {

        Socket sock = new Socket(ip, port);

        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        out.println(toJSON(request).toString());
        log.info("Sending " + request + " to " + ip + ":" + port, UserTransaction.class);

        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String inputStr = in.readLine();

        in.close();
        out.close();
        sock.close();
        Message response = parseJSON(new JSONObject(inputStr), expectedMessageType);
        log.info(response + " <-- " + request, UserTransaction.class);
        return response;

    }

    /**
     * Method to read the value of a key
     * @param key The key whose value is to be read
     * @return The value returned
     * @throws AbortException if the operation was unsuccessful
     */
    public Serializable read(K key) throws AbortException {

        if (status != TransactionStatus.live) {
            throw new TransactionNotLiveException();
        }

        ReadRequest request = new ReadRequest(transactionID, key, key.getLocalityHash());

        ReadResponse response;
        try {
            response = (ReadResponse) sendRequest(tmIp, tmPort, request, ReadResponse.class);
        } catch (Exception ex) {
            log.error("Remote error", ex, UserTransaction.class);
            throw new RemoteTMException(ex);
        }

        if (!response.getSuccess()) {
            status = TransactionStatus.aborted;
            log.warn(response.getExceptionMessage(), UserTransaction.class);
            throw new AbortToUserException(response.getExceptionMessage());
        }

        return response.getValue();

    }

    /**
     * Method to write value to a key.
     * @param key
     * @param value
     * @throws AbortException if the operation was unsuccessful
     */
    public void write(K key, Serializable value) throws AbortException {

        if (status != TransactionStatus.live) {
            throw new TransactionNotLiveException();
        }
        WriteRequest request = new WriteRequest(transactionID, key, value, key.getLocalityHash());
        GenericSuccessResponse response;
        try {
            response = (GenericSuccessResponse) sendRequest(tmIp, tmPort, request, GenericSuccessResponse.class);
        } catch (Exception ex) {
            log.error("Remote error", ex, UserTransaction.class);
            throw new RemoteTMException(ex);
        }

        if (!response.getSuccess()) {
            status = TransactionStatus.aborted;
            throw new AbortToUserException(response.getExceptionMessage());
        }

    }

    /**
     * Method to commit the transaction
     * @throws AbortException if the operation was unsuccessful
     */
    public void commit() throws AbortException {

        if (status != TransactionStatus.live) {
            throw new TransactionNotLiveException();
        }
        TryCommitRequest request = new TryCommitRequest(transactionID);
        GenericSuccessResponse response;
        try {
            response = (GenericSuccessResponse) sendRequest(tmIp, tmPort, request, GenericSuccessResponse.class);
        } catch (Exception ex) {
            log.error("Remote error", ex, UserTransaction.class);
            throw new RemoteTMException(ex);
        }

        if (!response.getSuccess()) {
            status = TransactionStatus.aborted;
            throw new AbortToUserException(response.getExceptionMessage());
        }

    }

    public int getTransactionID() {
        return transactionID;
    }

    /**
     * Method to abort transaction
     * @throws AbortException if the operation was unsuccessful
     */
    public void abort() throws AbortException {
        if (status != TransactionStatus.live) {
            throw new TransactionNotLiveException();

        }
        AbortRequest ar = new AbortRequest(transactionID);
        try {
            sendRequest(tmIp, tmPort, ar, GenericSuccessResponse.class);
            // TODO process response?

        } catch (Exception ex) {
            log.error("Remote error", ex, UserTransaction.class);
            throw new RemoteTMException(ex);
        }

    }
}
