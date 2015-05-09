package ch.epfl.tkvs.transactionmanager.algorithms;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.exceptions.CommitWithoutPrepareException;
import ch.epfl.tkvs.exceptions.TransactionAlreadyExistsException;
import ch.epfl.tkvs.exceptions.TransactionNotLiveException;
import ch.epfl.tkvs.exceptions.ValueDoesNotExistException;
import ch.epfl.tkvs.transactionmanager.Transaction;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.PrepareRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import ch.epfl.tkvs.transactionmanager.versioningunit.VersioningUnitMVTO;
import ch.epfl.tkvs.yarn.HDFSLogger;


public class MVTO extends CCAlgorithm {

    private VersioningUnitMVTO versioningUnit;

    public MVTO(RemoteHandler rh, HDFSLogger log) {
        super(rh, log);

        transactions = new ConcurrentHashMap<>();
        versioningUnit = VersioningUnitMVTO.getInstance();
        versioningUnit.init();
    }

    @Override
    public ReadResponse read(ReadRequest request) {
        int xid = request.getTransactionId();
        Serializable key = request.getEncodedKey();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new ReadResponse(new TransactionNotLiveException());
        }
        if (isLocalKey(request.getLocalityHash())) {
            Serializable value = versioningUnit.get(xid, key);
            if (value == null) {
                terminate(transaction, false);
                return new ReadResponse(new ValueDoesNotExistException());
            }
            return new ReadResponse((String) value);
        } else {
            return remote.read(transaction, request);
        }
    }

    @Override
    public GenericSuccessResponse write(WriteRequest request) {
        int xid = request.getTransactionId();
        Serializable key = request.getEncodedKey();
        Serializable value = request.getEncodedValue();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(new TransactionNotLiveException());
        }
        if (isLocalKey(request.getLocalityHash())) {
            try {
                versioningUnit.put(xid, key, value);
                return new GenericSuccessResponse();
            } catch (AbortException e) {
                terminate(transaction, false);
                return new GenericSuccessResponse(e);
            }
        } else {
            return remote.write(transaction, request);
        }
    }

    @Override
    public GenericSuccessResponse begin(BeginRequest request) {
        int xid = request.getTransactionId();

        // Transaction with duplicate id
        if (transactions.containsKey(xid)) {
            return new GenericSuccessResponse(new TransactionAlreadyExistsException());
        }
        transactions.put(xid, new Transaction(xid));

        versioningUnit.beginTransaction(xid);

        return new GenericSuccessResponse();
    }

    @Override
    public GenericSuccessResponse commit(CommitRequest request) {
        int xid = request.getTransactionId();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(new TransactionNotLiveException());
        }

        if (!transaction.isPrepared) {
            return new GenericSuccessResponse(new CommitWithoutPrepareException());
        }
        terminate(transaction, true);
        return new GenericSuccessResponse();

    }

    private ConcurrentHashMap<Integer, Transaction> transactions;

    // Does cleaning up after end of transaction
    private void terminate(Transaction transaction, boolean success) {
        log.info("Terminating transaction with status " + success, MVTO.class);
        if (success) {
            versioningUnit.commit(transaction.transactionId);
        } else {
            versioningUnit.abort(transaction.transactionId);
        }
        if (!success && !isLocalTransaction(transaction))
            remote.abortOthers(transaction);
        transactions.remove(transaction.transactionId);
    }

    @Override
    public GenericSuccessResponse abort(AbortRequest request) {
        int xid = request.getTransactionId();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(new TransactionNotLiveException());
        }

        terminate(transaction, false);
        return new GenericSuccessResponse();
    }

    @Override
    public GenericSuccessResponse prepare(PrepareRequest request) {
        int xid = request.getTransactionId();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(new TransactionNotLiveException());
        }
        try {
            versioningUnit.prepareCommit(xid);
            transaction.isPrepared = true;
            return new GenericSuccessResponse();
        } catch (AbortException e) {
            terminate(transaction, false);
            return new GenericSuccessResponse(e);
        }
    }

    @Override
    public Transaction getTransaction(int xid) {
        return transactions.get(xid);
    }

    @Override
    public void checkpoint() {
    }

}
