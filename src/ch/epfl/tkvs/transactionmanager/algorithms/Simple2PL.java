package ch.epfl.tkvs.transactionmanager.algorithms;

import java.io.Serializable;
import java.util.Arrays;

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
import ch.epfl.tkvs.transactionmanager.lockingunit.LockType.Default;
import ch.epfl.tkvs.yarn.HDFSLogger;


public class Simple2PL extends Algo2PL {

    public Simple2PL(RemoteHandler rh, HDFSLogger log) {
        super(rh, log);
        lockingUnit.init();
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
        // if the key is stored locally, process it locally or use remote handler to forward the request to the correct
        // Tranasction manager.

        if (isLocalKey(request.getLocalityHash())) {
            LockType lock = Default.READ_LOCK;
            try {
                // if read or write lock is already held , do not request again
                if (!transaction.checkLock(key, lock) && !transaction.checkLock(key, Default.WRITE_LOCK)) {
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
            LockType lock = Default.WRITE_LOCK;
            try {
                // if lock is already held, do not request again
                if (!transaction.checkLock(key, lock)) {
                    lockingUnit.promote(xid, key, transaction.getLocksForKey(key), lock);
                    transaction.setLock(key, Arrays.asList(lock));
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
        transaction.isPrepared = true;
        return new GenericSuccessResponse();
    }

}
