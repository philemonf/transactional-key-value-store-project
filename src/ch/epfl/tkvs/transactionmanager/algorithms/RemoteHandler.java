package ch.epfl.tkvs.transactionmanager.algorithms;

import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.exceptions.PrepareException;
import ch.epfl.tkvs.exceptions.RemoteTMException;
import ch.epfl.tkvs.transactionmanager.Transaction;
import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.PrepareRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;


/**
 ** This class acts as a proxy for user client for distributed tranasactions running on secondary Transaction Managers.
 * Also responsbile for 2 Phase Commit protocol
 */
public class RemoteHandler {

    // The concurrency control algorithm being executed locally to which the remote handler is attached to.
    private CCAlgorithm localAlgo;
    private static final Logger log = Logger.getLogger(RemoteHandler.class);

    // public RemoteHandler(CCAlgorithm localAlgo)
    public void setAlgo(CCAlgorithm localAlgo) {
        this.localAlgo = localAlgo;
    }

    private Message sendToRemoteTM(Message request, int localityHash, Class<? extends Message> messageClass) throws IOException, InvalidMessageException {

        log.info(request);
        JSONObject response = TransactionManager.sendToTransactionManager(localityHash, request, true);
        Message responseMessage = parseJSON(response, messageClass);
        log.info(response + "<--" + request);
        return responseMessage;

    }

    private void sendToRemoteTM(Message request, int localityHash) throws IOException {
        log.info(request);
        TransactionManager.sendToTransactionManager(localityHash, request, false);
    }

    /**
     * Initiates a transaction on secondary Transaction Manager for distributed transaction
     *
     * @param t The transaction running on primary Transaction Manager
     * @param hash The hash code of key for identifying the remote Transaction Manager
     * @return the response from the secondary Transaction Manager
     */
    private void begin(Transaction t, int hash) throws IOException, InvalidMessageException, AbortException {
        if (!t.remoteIsPrepared.containsKey(hash)) {
            GenericSuccessResponse response = (GenericSuccessResponse) sendToRemoteTM(new BeginRequest(t.transactionId), hash, GenericSuccessResponse.class);
            if (!response.getSuccess()) {

                throw new RemoteTMException(response.getExceptionMessage());
            }
            t.remoteIsPrepared.put(hash, Boolean.FALSE);
        }

    }

    /**
     * Performs a remote read on secondary Transaction Manager for distributed transaction Invokes distributed abort in
     * case of error
     *
     * @param t The transaction running on primary Transaction Manager
     * @param request the original request received by primary Transaction Manager
     * @return the response from the secondary Transaction Manager
     */
    public ReadResponse read(Transaction t, ReadRequest request) {

        int id = request.getTransactionId();
        int tmHash = request.getLocalityHash();
        try {
            begin(t, tmHash);
            ReadResponse rr = (ReadResponse) sendToRemoteTM(request, tmHash, ReadResponse.class);
            if (!rr.getSuccess()) {
                throw new RemoteTMException(rr.getExceptionMessage());
            }
            return rr;
        } catch (IOException | InvalidMessageException ex) {
            log.fatal("Remote error", ex);
            abortAll(t);
            return new ReadResponse(new RemoteTMException(ex.getMessage()));
        } catch (AbortException e) {
            abortAll(t);
            return new ReadResponse(e);
        }

    }

    /**
     * Performs a remote write on secondary Transaction Manager for distributed transaction Invokes distributed abort in
     * case of error
     *
     * @param t The transaction running on primary Transaction Manager
     * @param request the original request received by primary Transaction Manager
     * @return the response from the secondary Transaction Manager
     */
    public GenericSuccessResponse write(Transaction t, WriteRequest request) {
        int id = request.getTransactionId();
        int tmHash = request.getLocalityHash();
        try {
            begin(t, tmHash);

            GenericSuccessResponse gsr = (GenericSuccessResponse) sendToRemoteTM(request, tmHash, GenericSuccessResponse.class);
            if (!gsr.getSuccess()) {
                throw new RemoteTMException(gsr.getExceptionMessage());
            }
            return gsr;
        } catch (IOException | InvalidMessageException ex) {
            log.fatal("Remote error", ex);
            abortAll(t);
            return new GenericSuccessResponse(new RemoteTMException(ex.getMessage()));
        } catch (AbortException ex) {
            abortAll(t);
            return new GenericSuccessResponse(ex);
        }
    }

    /**
     * Performs 2-Phase commit protocol to try to commit a distributed transaction Invokes distributed abort in case of
     * error
     *
     * @param t The transaction running on primary Transaction Manager
     * @return the response from the secondary Transaction Manager
     */
    public GenericSuccessResponse tryCommit(Transaction t) {
        PrepareRequest pr = new PrepareRequest(t.transactionId);
        GenericSuccessResponse response = localAlgo.prepare(pr);

        try {
            if (!response.getSuccess())
                throw new PrepareException(response.getExceptionMessage());
            for (Integer remoteHash : t.remoteIsPrepared.keySet()) {
                response = (GenericSuccessResponse) sendToRemoteTM(pr, remoteHash, GenericSuccessResponse.class);
                if (!response.getSuccess()) {
                    throw new PrepareException(response.getExceptionMessage());
                }

            }

            CommitRequest cr = new CommitRequest(t.transactionId);
            localAlgo.commit(cr);
            commitOthers(t);

            return new GenericSuccessResponse();

        } catch (IOException | InvalidMessageException ex) {
            abortAll(t);
            log.fatal("remote error", ex);
            return new GenericSuccessResponse(new RemoteTMException(ex.getMessage()));
        } catch (AbortException ex) {
            abortAll(t);
            return new GenericSuccessResponse(ex);
        }

    }

    /**
     * Sends commit message to secondary Transaction Managers. TODO: ensure that all commits are successful
     * @param t Transaction to be committed
     */
    private void commitOthers(Transaction t) throws IOException {
        CommitRequest cr = new CommitRequest(t.transactionId);
        for (Integer remoteHash : t.remoteIsPrepared.keySet()) {
            sendToRemoteTM(cr, remoteHash);
            // TODO: check response and do something?
        }
    }

    // Sends abort message to local algorithm which in turn would invoke abortOthers()
    private void abortAll(Transaction t) {
        AbortRequest ar = new AbortRequest(t.transactionId);
        localAlgo.abort(ar);
    }

    // TODO: return true or false?
    /**
     * Sends abort message to secondary Transaction Managers
     *
     * @param t Transaction to be committed
     * 
     */
    public void abortOthers(Transaction t) {
        if (t.areAllRemoteAborted) {
            return;
        }
        AbortRequest ar = new AbortRequest(t.transactionId);
        try {
            for (Integer remoteHash : t.remoteIsPrepared.keySet()) {
                sendToRemoteTM(ar, remoteHash);
                // TODO: check response and do something?
            }
        } catch (IOException ex) {
            // TODO: check response and do something?

        }
        t.areAllRemoteAborted = true;

    }
}
