package ch.epfl.tkvs.transactionmanager.algorithms;

import static ch.epfl.tkvs.transactionmanager.lockingunit.LockCompatibilityTable.newCompatibilityList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.exceptions.TransactionNotLiveException;
import ch.epfl.tkvs.exceptions.ValueDoesNotExistException;
import ch.epfl.tkvs.transactionmanager.Transaction_2PL;
import ch.epfl.tkvs.transactionmanager.communication.requests.PrepareRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockType;
import ch.epfl.tkvs.yarn.HDFSLogger;


public class MVCC2PL extends Algo2PL {

    public MVCC2PL(RemoteHandler rh, HDFSLogger log) {
        super(rh, log);

        Map<LockType, List<LockType>> lockCompatibility = new HashMap<LockType, List<LockType>>();
        lockCompatibility.put(Lock.READ_LOCK, newCompatibilityList(Lock.READ_LOCK, Lock.WRITE_LOCK));
        lockCompatibility.put(Lock.WRITE_LOCK, newCompatibilityList(Lock.READ_LOCK));
        lockCompatibility.put(Lock.COMMIT_LOCK, newCompatibilityList());
        lockingUnit.initWithLockCompatibilityTable(lockCompatibility);

    }

    @Override
    public ReadResponse read(ReadRequest request) {
        int xid = request.getTransactionId();
        Serializable key = request.getEncodedKey();

        Transaction_2PL transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new ReadResponse(new TransactionNotLiveException());
        }
        if (isLocalKey(request.getLocalityHash())) {
            Lock lock = MVCC2PL.Lock.READ_LOCK;
            try {
                if (!transaction.checkLock(key, lock)) {
                    lockingUnit.lock(xid, key, lock);
                    transaction.addLock(key, lock);
                }
                Serializable value = versioningUnit.get(xid, key);
                if (value == null)
                    throw new ValueDoesNotExistException();
                return new ReadResponse((String) value);
            } catch (AbortException e) {
                terminate(transaction, false);
                return new ReadResponse(e);
            }
        } else {
            return remote.read(transaction, request);
        }
    }

    @Override
    public GenericSuccessResponse write(WriteRequest request) {
        int xid = request.getTransactionId();
        Serializable key = request.getEncodedKey();
        Serializable value = request.getEncodedValue();

        Transaction_2PL transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(new TransactionNotLiveException());
        }
        if (isLocalKey(request.getLocalityHash())) {
            Lock lock = Lock.WRITE_LOCK;
            try {
                if (!transaction.checkLock(key, lock)) {
                    lockingUnit.lock(xid, key, lock);
                    transaction.addLock(key, lock);
                }
                versioningUnit.put(xid, key, value);
                return new GenericSuccessResponse();
            } catch (AbortException e) {
                terminate(transaction, false);
                return new GenericSuccessResponse(e);
            }
        } else
            return remote.write(transaction, request);
    }

    @Override
    public GenericSuccessResponse prepare(PrepareRequest request) {
        int xid = request.getTransactionId();

        Transaction_2PL transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(new TransactionNotLiveException());
        }
        try {
            for (Serializable key : transaction.getLockedKeys()) {

                // promote each write lock to commit lock
                if (transaction.getLocksForKey(key).contains(Lock.WRITE_LOCK)) {
                    lockingUnit.promote(xid, key, transaction.getLocksForKey(key), Lock.COMMIT_LOCK);
                    // remove old locks
                    transaction.setLock(key, Arrays.asList((LockType) Lock.COMMIT_LOCK));
                }
            }
            transaction.isPrepared = true;
            return new GenericSuccessResponse();

        } catch (AbortException e) {
            terminate(transaction, false);
            return new GenericSuccessResponse(e);
        }
    }

    private static enum Lock implements LockType {

        READ_LOCK, WRITE_LOCK, COMMIT_LOCK
    }

}
