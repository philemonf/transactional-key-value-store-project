package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.transactionmanager.Transaction;
import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
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
import org.codehaus.jettison.json.JSONObject;

import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;
import java.io.IOException;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONException;


/**
 ** This class acts as a proxy for user client for distributed tranasactions running on secondary Transaction Managers.
 * Also responsbile for 2 Phase Commit protocol
 */
public class RemoteHandler {

    // The concurrency control algorithm being executed locally to which the remote handler is attached to.
    private CCAlgorithm localAlgo;

    // public RemoteHandler(CCAlgorithm localAlgo)
    public void setAlgo(CCAlgorithm localAlgo) {
        this.localAlgo = localAlgo;
    }

    private JSONObject sendToRemoteTM(Message m, int hash, boolean shouldWaitforResponse) throws IOException {
        return null;
    }

    /**
     * Initiates a transaction on secondary Transaction Manager for distributed transaction
     * @param t The transaction running on primary Transaction Manager
     * @param hash The hash code of key for identifying the remote Transaction Manager
     * @return the response from the secondary Transaction Manager
     */
    private boolean begin(Transaction t, int hash) {
        if (!t.remoteIsPrepared.containsKey(hash)) {
            try {
                JSONObject response = sendToRemoteTM(new BeginRequest(t.transactionId), hash, true);
                boolean success = false;

                success = response.getBoolean(JSONCommunication.KEY_FOR_SUCCESS);
                t.remoteIsPrepared.put(hash, Boolean.FALSE);
                return success;
            } catch (IOException | JSONException ex) {
                Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);
                return false;
            }
        }
        return true;
    }

    /**
     * Performs a remote read on secondary Transaction Manager for distributed transaction Invokes distributed abort in
     * case of error
     * @param t The transaction running on primary Transaction Manager
     * @param request the original request received by primary Transaction Manager
     * @return the response from the secondary Transaction Manager
     */
    public ReadResponse read(Transaction t, ReadRequest request) {

        int id = request.getTransactionId();
        int tmHash = request.getTMhash();
        if (!begin(t, tmHash)) {
            return new ReadResponse(false, null);
        }
        try {
            ReadResponse rr = (ReadResponse) parseJSON(sendToRemoteTM(request, tmHash, true), ReadResponse.class);
            if (!rr.getSuccess())
                abort(t);
            return rr;
        } catch (IOException | InvalidMessageException ex) {
            Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);
            return new ReadResponse(false, null);
        }

    }

    /**
     * Performs a remote write on secondary Transaction Manager for distributed transaction Invokes distributed abort in
     * case of error
     * @param t The transaction running on primary Transaction Manager
     * @param request the original request received by primary Transaction Manager
     * @return the response from the secondary Transaction Manager
     */
    public GenericSuccessResponse write(Transaction t, WriteRequest request) {
        int id = request.getTransactionId();
        int tmHash = request.getTMhash();
        if (!begin(t, tmHash)) {
            return new GenericSuccessResponse(false);
        }
        try {
            GenericSuccessResponse gsr = (GenericSuccessResponse) parseJSON(sendToRemoteTM(request, tmHash, true), GenericSuccessResponse.class);
            if (!gsr.getSuccess())
                abort(t);
            return gsr;
        } catch (IOException | InvalidMessageException ex) {
            Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);
            return new GenericSuccessResponse(false);
        }
    }

    /**
     * Performs 2-Phase commit protocol to try to commit a distributed transaction Invokes distributed abort in case of
     * error
     * @param t The transaction running on primary Transaction Manager
     * @param request the original request received by primary Transaction Manager
     * @return the response from the secondary Transaction Manager
     */
    public GenericSuccessResponse tryCommit(Transaction t) {
        PrepareRequest pr = new PrepareRequest(t.transactionId);
        boolean canCommit = localAlgo.prepare(pr).getSuccess();
        for (Integer remoteHash : t.remoteIsPrepared.keySet()) {

            if (!canCommit)
                break;

            boolean response = false;
            try {
                response = sendToRemoteTM(pr, remoteHash, true).getBoolean(JSONCommunication.KEY_FOR_SUCCESS);
            } catch (IOException | JSONException ex) {
                Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);

            }
            canCommit &= response;

        }
        if (canCommit) {
            try {
                CommitRequest cr = new CommitRequest(t.transactionId);
                localAlgo.commit(cr);
                commit(t);
            } catch (Exception ex) {
                // TODO: something in case of network error? return true or false?
                Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);
                return new GenericSuccessResponse(false);
            }
        } else {
            AbortRequest ar = new AbortRequest(t.transactionId);
            localAlgo.abort(ar);
            abort(t);
        }
        return new GenericSuccessResponse(canCommit);

    }

    /**
     * Sends commit message to secondary Transaction Managers
     * @param t Transaction to be committed
     */
    private void commit(Transaction t) throws IOException {
        CommitRequest cr = new CommitRequest(t.transactionId);
        for (Integer remoteHash : t.remoteIsPrepared.keySet()) {
            sendToRemoteTM(cr, remoteHash, false);
            // TODO: check response and do something?
        }
    }

    // TODO: return true or false?
    /**
     * Sends abort message to secondary Transaction Managers
     * @param t Transaction to be committed
     * @return response to be sent
     **/
    public GenericSuccessResponse abort(Transaction t) {
        if (t.areAllRemoteAborted)
            return new GenericSuccessResponse(true);
        AbortRequest ar = new AbortRequest(t.transactionId);
        try {
            for (Integer remoteHash : t.remoteIsPrepared.keySet()) {
                sendToRemoteTM(ar, remoteHash, false);
                // TODO: check response and do something?
            }
        } catch (IOException ex) {
            return new GenericSuccessResponse(false);
        }
        t.areAllRemoteAborted = true;
        return new GenericSuccessResponse(true);
    }
}
