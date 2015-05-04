package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.transactionmanager.Transaction;
import ch.epfl.tkvs.transactionmanager.communication.DeadlockInfoMessage;
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
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter;
import org.codehaus.jettison.json.JSONObject;

import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONException;


public class RemoteHandler {

    private CCAlgorithm localAlgo;

    public RemoteHandler(CCAlgorithm localAlgo) {
        this.localAlgo = localAlgo;
    }

    private JSONObject sendToRemoteTM(Message m, int hash) {
        return null;
    }

    private boolean begin(Transaction t, int hash) {
        if (!t.remoteIsPrepared.containsKey(hash)) {
            JSONObject response = sendToRemoteTM(new BeginRequest(t.transactionId), hash);
            boolean success = false;
            try {
                success = response.getBoolean(JSONCommunication.KEY_FOR_SUCCESS);
                t.remoteIsPrepared.put(hash, Boolean.FALSE);
                return success;
            } catch (JSONException ex) {
                Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return true;
    }

    public ReadResponse read(Transaction t, ReadRequest request) {

        int id = request.getTransactionId();
        int tmHash = request.getTMhash();
        if (!begin(t, tmHash)) {
            return new ReadResponse(false, null);
        }
        try {
            ReadResponse rr = (ReadResponse) parseJSON(sendToRemoteTM(request, tmHash), ReadResponse.class);
            if (!rr.getSuccess())
                abort(t);
            return rr;
        } catch (JSON2MessageConverter.InvalidMessageException ex) {
            Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);
            return new ReadResponse(false, null);
        }

    }

    public GenericSuccessResponse write(Transaction t, WriteRequest request) {
        int id = request.getTransactionId();
        int tmHash = request.getTMhash();
        if (!begin(t, tmHash)) {
            return new GenericSuccessResponse(false);
        }
        try {
            GenericSuccessResponse gsr = (GenericSuccessResponse) parseJSON(sendToRemoteTM(request, tmHash), GenericSuccessResponse.class);
            if (!gsr.getSuccess())
                abort(t);
            return gsr;
        } catch (JSON2MessageConverter.InvalidMessageException ex) {
            Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);
            return new GenericSuccessResponse(false);
        }
    }

    public GenericSuccessResponse tryCommit(Transaction t) {
        PrepareRequest pr = new PrepareRequest(t.transactionId);
        boolean canCommit = localAlgo.prepare(pr).getSuccess();
        for (Integer remoteHash : t.remoteIsPrepared.keySet()) {

            if (!canCommit)
                break;

            boolean response = false;
            try {
                response = sendToRemoteTM(pr, remoteHash).getBoolean(JSONCommunication.KEY_FOR_SUCCESS);
            } catch (JSONException ex) {
                Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);

            }
            canCommit &= response;

        }
        if (canCommit) {
            CommitRequest cr = new CommitRequest(t.transactionId);
            localAlgo.commit(cr);
            commit(t);
        } else {
            AbortRequest ar = new AbortRequest(t.transactionId);
            localAlgo.abort(ar);
            abort(t);
        }
        return new GenericSuccessResponse(canCommit);

    }

    private void commit(Transaction t) {
        CommitRequest cr = new CommitRequest(t.transactionId);
        for (Integer remoteHash : t.remoteIsPrepared.keySet()) {
            sendToRemoteTM(cr, remoteHash);
        }
    }

    // FIXME return true or false?
    public GenericSuccessResponse abort(Transaction t) {
        if (t.areAllRemoteAborted)
            return new GenericSuccessResponse(true);
        AbortRequest ar = new AbortRequest(t.transactionId);
        for (Integer remoteHash : t.remoteIsPrepared.keySet()) {
            sendToRemoteTM(ar, remoteHash);
        }
        t.areAllRemoteAborted = true;
        return new GenericSuccessResponse(true);
    }
}
