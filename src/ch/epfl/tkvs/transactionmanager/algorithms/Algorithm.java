/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.transactionmanager.Transaction;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.PrepareRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.TryCommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;


public abstract class Algorithm {

    protected RemoteHandler remote;

    public abstract ReadResponse read(ReadRequest request);

    public abstract GenericSuccessResponse write(WriteRequest request);

    public abstract GenericSuccessResponse begin(BeginRequest request);

    public abstract GenericSuccessResponse commit(CommitRequest request);

    public abstract GenericSuccessResponse abort(AbortRequest request);

    public abstract GenericSuccessResponse prepare(PrepareRequest request);

    public abstract Transaction getTransaction(int xid);

    public GenericSuccessResponse tryCommit(TryCommitRequest request) {
        int xid = request.getTransactionId();

        Transaction transaction = getTransaction(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(false);
        }

        if (isLocalTransaction(transaction)) {
            prepare(new PrepareRequest(xid));
            return commit(new CommitRequest(xid));
        } else
            return remote.tryCommit(transaction);
    }

    public Algorithm(RemoteHandler remote) {
        this.remote = remote;
    }

    protected boolean isLocalKey(int hash) {
        return (remote == null);
    }

    protected boolean isLocalTransaction(Transaction t) {
        return (remote == null) || (t.remoteIsPrepared.isEmpty());
    }
}
