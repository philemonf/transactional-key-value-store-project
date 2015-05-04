package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.transactionmanager.Transaction;
import ch.epfl.tkvs.transactionmanager.Transaction_2PL;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockType;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockingUnit;
import ch.epfl.tkvs.transactionmanager.versioningunit.VersioningUnitMVCC2PL;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public abstract class Algo2PL extends Algorithm {

    protected LockingUnit lockingUnit;
    protected VersioningUnitMVCC2PL versioningUnit;

    protected ConcurrentHashMap<Integer, Transaction_2PL> transactions;

    public Algo2PL(RemoteHandler remote) {
        super(remote);
        lockingUnit = LockingUnit.instance;
        versioningUnit = VersioningUnitMVCC2PL.getInstance();
        versioningUnit.init();
        transactions = new ConcurrentHashMap<>();

    }

    // Does cleaning up after end of transaction
    protected void terminate(Transaction_2PL transaction, boolean success) {
        if (success) {
            versioningUnit.commit(transaction.transactionId);
        } else {
            versioningUnit.abort(transaction.transactionId);

        }
        lockingUnit.releaseAll(transaction.transactionId, transaction.getCurrentLocks());
        if (!success && !isLocalTransaction(transaction))
            remote.abort(transaction);
        transactions.remove(transaction.transactionId);
    }

    @Override
    public Transaction getTransaction(int xid) {
        return transactions.get(xid);
    }

    @Override
    public GenericSuccessResponse abort(AbortRequest request) {
        int xid = request.getTransactionId();

        Transaction_2PL transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(false);
        }
        terminate(transaction, false);
        return new GenericSuccessResponse(true);
    }

    @Override
    public GenericSuccessResponse begin(BeginRequest request) {
        int xid = request.getTransactionId();

        // Transaction with duplicate id
        if (transactions.containsKey(xid)) {
            return new GenericSuccessResponse(false);
        }
        transactions.put(xid, new Transaction_2PL(xid));

        return new GenericSuccessResponse(true);
    }

    @Override
    public GenericSuccessResponse commit(CommitRequest request) {
        int xid = request.getTransactionId();

        Transaction_2PL transaction = transactions.get(xid);

        // Transaction not begun or already terminated or not prepared
        if (transaction == null || !transaction.isPrepared) {
            return new GenericSuccessResponse(false);
        }
        terminate(transaction, true);
        return new GenericSuccessResponse(true);

    }

}
