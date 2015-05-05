/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.transactionmanager.Transaction;
import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.PrepareRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.TryCommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;


/**
 * This abstract class represents an concurrency control algorithm. One must come up with actual implementation of such
 * a class and inject it in the transaction manager in order to get a custom concurrency algorithm.
 */
public abstract class CCAlgorithm {

    protected RemoteHandler remote;

    /**
     * Called whenever the transaction manager receives a read request
     * @param request the incoming read request
     * @return the response to be sent to the sender
     */
    public abstract ReadResponse read(ReadRequest request);

    /**
     * Called whenever the transaction manager receives a write request.
     * @param request the incoming write request
     * @return the response to be sent to the sender
     */
    public abstract GenericSuccessResponse write(WriteRequest request);

    /**
     * Called whenever the transaction manager receives a request to begin a transaction
     * @param request the incoming begin request
     * @return the response to be sent to the sender
     */
    public abstract GenericSuccessResponse begin(BeginRequest request);

    /**
     * Called whenever the transaction manager receives a commit request.
     * @param request the incoming commit request
     * @return the response to be sent to the sender
     */
    public abstract GenericSuccessResponse commit(CommitRequest request);

    /**
     * Called whenever the transaction manager receives an abort request.
     * @param request the incoming abort request
     * @return the response to be sent to the sender
     */
    public abstract GenericSuccessResponse abort(AbortRequest request);

    /**
     * Called whenever the transaction manager receives a prepare request (first phase of 2PC).
     * @param request the incoming prepare request
     * @return the response to be sent to the sender
     */
    public abstract GenericSuccessResponse prepare(PrepareRequest request);

    /**
     * Returns a transaction object given its id.
     * @param xid the id of the transaction
     * @return the corresponding transaction
     */
    public abstract Transaction getTransaction(int xid);

    /**
     * Called by user client to prepare and commit the transaction, using 2-Phase Commit protocol in case of distributed
     * transaction
     * @param request incoming TryCommitRequests
     * @return the response to be sent to the sender
     */
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

    /**
     * This method is called periodically by the transaction manager. It can be used for a lot of purpose including:
     * <ul> <li>Fault tolerance</li> <li>Sending report to some node</li> <li>Internal state audit</li> </ul> This is up
     * to the actual implementation of the concurrency control algorithm to decide. If such a function is not needed,
     * please leave it empty.
     */
    abstract public void checkpoint();

    public CCAlgorithm(RemoteHandler remote) {
        this.remote = remote;
    }

    protected boolean isLocalKey(int localityHash) {
        return (remote == null) || localityHash == TransactionManager.getLocalityHash();
    }

    protected boolean isLocalTransaction(Transaction t) {
        return (remote == null) || (t.remoteIsPrepared.isEmpty());
    }
}
