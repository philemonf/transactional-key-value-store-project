package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;
import ch.epfl.tkvs.transactionmanager.AbortException;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.PrepareRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import static ch.epfl.tkvs.transactionmanager.lockingunit.LockType.Default;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockType;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockingUnit;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class Simple2PL implements Algorithm {

    private LockingUnit lockingUnit;
    private KeyValueStore KVS;

    public Simple2PL() {
        lockingUnit = LockingUnit.instance;
        KVS = KeyValueStore.instance;
        KVS.clear();
        lockingUnit.init();
        transactions = new ConcurrentHashMap<>();
    }

    @Override
    public ReadResponse read(ReadRequest request) {
        int xid = request.getTransactionId();
        Serializable key = request.getEncodedKey();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new ReadResponse(false, null);
        }

        LockType lock = Default.READ_LOCK;
        try {
            if (!transaction.checkLock(key, lock)) {
                lockingUnit.lock(xid, key, lock);
                transaction.addLock(key, lock);
            }
            Serializable value = KVS.get(key);
            return new ReadResponse(true, (String) value);
        } catch (AbortException e) {
            terminate(transaction, false);
            return new ReadResponse(false, null);
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
            return new GenericSuccessResponse(false);
        }

        LockType lock = Default.WRITE_LOCK;
        try {
            if (!transaction.checkLock(key, lock)) {
                lockingUnit.promote(xid, key, transaction.getLocksForKey(key), lock);
                transaction.setLock(key, Arrays.asList(lock));
            }
            KVS.put(key, value);
            return new GenericSuccessResponse(true);
        } catch (AbortException e) {
            terminate(transaction, false);
            return new GenericSuccessResponse(false);
        }
    }

    @Override
    public GenericSuccessResponse begin(BeginRequest request) {
        int xid = request.getTransactionId();

        // Transaction with duplicate id
        if (transactions.containsKey(xid)) {
            return new GenericSuccessResponse(false);
        }
        transactions.put(xid, new Transaction(xid));

        return new GenericSuccessResponse(true);
    }

    @Override
    public GenericSuccessResponse commit(CommitRequest request) {
        int xid = request.getTransactionId();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null || !transaction.isPrepared) {
            return new GenericSuccessResponse(false);
        }

        terminate(transaction, true);
        return new GenericSuccessResponse(true);

    }

    private ConcurrentHashMap<Integer, Transaction> transactions;

    // Does cleaning up after end of transaction
    private void terminate(Transaction transaction, boolean success) {
        lockingUnit.releaseAll(transaction.transactionId, transaction.currentLocks);
        transactions.remove(transaction.transactionId);
    }

    @Override
    public GenericSuccessResponse abort(AbortRequest request) {
        terminate(null, true);
        return null;
    }

    @Override
    public GenericSuccessResponse prepare(PrepareRequest request) {
        int xid = request.getTransactionId();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(false);
        }
        transaction.isPrepared = true;
        return new GenericSuccessResponse(true);
    }

    private class Transaction {

        private int transactionId;
        boolean isPrepared;
        private HashMap<Serializable, List<LockType>> currentLocks;

        public Transaction(int transactionId) {
            this.transactionId = transactionId;
            isPrepared = false;
            currentLocks = new HashMap<>();
        }

        /**
         * Replaces old locks for a key with new locks
         *
         * @param key
         * @param newLocks
         */
        public void setLock(Serializable key, List<LockType> newLocks) {
            currentLocks.put(key, newLocks);
        }

        /**
         * Adds lock of type lockType for a key
         *
         * @param key
         * @param lockType
         */
        public void addLock(Serializable key, LockType lockType) {
            if (currentLocks.containsKey(key)) {
                currentLocks.get(key).add(lockType);
            } else {
                currentLocks.put(key, new LinkedList<LockType>(Arrays.asList(lockType)));
            }
        }

        public boolean checkLock(Serializable key, LockType lockType) {
            return currentLocks.get(key) != null && (currentLocks.get(key).contains(lockType) || currentLocks.get(key).contains(Default.WRITE_LOCK));
        }

        public Set<Serializable> getLockedKeys() {
            return currentLocks.keySet();
        }

        public List<LockType> getLocksForKey(Serializable key) {
            return currentLocks.get(key);
        }
    }
}
