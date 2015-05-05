package ch.epfl.tkvs.transactionmanager.algorithms;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import ch.epfl.tkvs.transactionmanager.Transaction;
import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.Transaction_2PL;
import ch.epfl.tkvs.transactionmanager.communication.DeadlockInfoMessage;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.lockingunit.DeadlockGraph;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockingUnit;
import ch.epfl.tkvs.transactionmanager.versioningunit.VersioningUnitMVCC2PL;


public abstract class Algo2PL extends CCAlgorithm {

    protected LockingUnit lockingUnit;
    protected VersioningUnitMVCC2PL versioningUnit;

    protected ConcurrentHashMap<Integer, Transaction_2PL> transactions;
    private final static Logger log = Logger.getLogger(Algo2PL.class.getName());

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
        if (!lockingUnit.interruptWaitingLocks(xid))
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

    @Override
    public void checkpoint() {
        // Get the dead lock graph
        DeadlockGraph graph = lockingUnit.getDeadlockGraph();

        try {
            // Create the message
            DeadlockInfoMessage deadlockMessage = new DeadlockInfoMessage(graph);
            log.info("About to send deadlock info to app master: " + deadlockMessage);
            TransactionManager.sendToAppMaster(deadlockMessage);

        } catch (IOException e) {
            log.error(e);
        }
    }

}
