package ch.epfl.tkvs.transactionmanager;

import ch.epfl.tkvs.transactionmanager.algorithms.Algorithm;
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
import ch.epfl.tkvs.transactionmanager.lockingunit.LockingUnit;
import org.codehaus.jettison.json.JSONObject;

import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONException;


public class RemoteHandler {

    private Algorithm localAlgorithm;
    private LockingUnit localLU;

    private boolean isLocalKey(int hash) {
        return true;
    }

    private boolean isLocalTransaction(int xid) {
        return localAlgorithm.getTransaction(xid).remoteIsPrepared.isEmpty();
    }

    private JSONObject sendToRemoteTM(Message m, int hash) {
        return null;
    }

    private GenericSuccessResponse distr_commit() {
        return null;
    }

    private GenericSuccessResponse distr_abort() {
        return null;
    }

    private boolean initiateRemote(int xid, int hash) {
        if (!localAlgorithm.getTransaction(xid).remoteIsPrepared.containsKey(hash)) {
            JSONObject response = sendToRemoteTM(new BeginRequest(xid, false), hash);
            boolean success = false;
            try {
                success = response.getBoolean(JSONCommunication.KEY_FOR_SUCCESS);
                localAlgorithm.getTransaction(xid).remoteIsPrepared.put(xid, Boolean.FALSE);
                return success;
            } catch (JSONException ex) {
                Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return true;
    }

    public RemoteHandler(Algorithm localAlgorithm) {
        this.localAlgorithm = localAlgorithm;
        localLU = null;
    }

    public RemoteHandler(Algorithm localAlgorithm, LockingUnit LU) {
        this.localAlgorithm = localAlgorithm;
        localLU = LU;
    }

    public ReadResponse read(ReadRequest request) {
        if (isLocalKey(request.getTMhash()))
            return localAlgorithm.read(request);

        int id = request.getTransactionId();
        int tmHash = request.getTMhash();
        if (!initiateRemote(id, tmHash)) {
            return new ReadResponse(false, null);
        }
        try {
            ReadResponse rr = (ReadResponse) parseJSON(sendToRemoteTM(request, tmHash), ReadResponse.class);
            if (!rr.getSuccess())
                distr_abort();
            return rr;
        } catch (JSON2MessageConverter.InvalidMessageException ex) {
            Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);
            return new ReadResponse(false, null);
        }

    }

    public GenericSuccessResponse write(WriteRequest request) {
        if (isLocalKey(request.getTMhash()))
            return localAlgorithm.write(request);

        int id = request.getTransactionId();
        int tmHash = request.getTMhash();
        if (!initiateRemote(id, tmHash)) {
            return new GenericSuccessResponse(false);
        }
        try {
            GenericSuccessResponse gsr = (GenericSuccessResponse) parseJSON(sendToRemoteTM(request, tmHash), GenericSuccessResponse.class);
            if (!gsr.getSuccess())
                distr_abort();
            return gsr;
        } catch (JSON2MessageConverter.InvalidMessageException ex) {
            Logger.getLogger(RemoteHandler.class.getName()).log(Level.SEVERE, null, ex);
            return new GenericSuccessResponse(false);
        }
    }

    public GenericSuccessResponse begin(BeginRequest request) {
        return localAlgorithm.begin(request);
    }

    public GenericSuccessResponse commit(CommitRequest request) {
        if (!request.isPrimaryMessage()) {
            return localAlgorithm.commit(request);

        }
        int xid = request.getTransactionId();
        if (isLocalTransaction(xid)) {
            localAlgorithm.prepare(new PrepareRequest(xid));
            return localAlgorithm.commit(request);
        }
        return distr_commit();
    }

    public GenericSuccessResponse abort(AbortRequest request) {
        if (!request.isPrimaryMessage() || isLocalTransaction(request.getTransactionId()))
            return localAlgorithm.abort(request);
        return distr_abort();
    }

    public GenericSuccessResponse prepare(PrepareRequest request) {
        return localAlgorithm.prepare(request);
    }

}
