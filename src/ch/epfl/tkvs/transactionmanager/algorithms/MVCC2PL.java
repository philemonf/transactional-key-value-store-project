
package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockType;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockingUnit;
import ch.epfl.tkvs.transactionmanager.versioningunit.VersioningUnit;
import static ch.epfl.tkvs.transactionmanager.lockingunit.LockCompatibilityTable.newCompatibilityList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class MVCC2PL implements Algorithm
  {

    private LockingUnit lockingUnit;
    private VersioningUnit versioningUnit;

    public MVCC2PL()
      {
        
        lockingUnit = LockingUnit.instance;
        
        HashMap<LockType, List<LockType>> lockCompatibility = new HashMap<>();
        lockCompatibility.put(Lock.READ_LOCK, newCompatibilityList(Lock.READ_LOCK, Lock.WRITE_LOCK));
        lockCompatibility.put(Lock.WRITE_LOCK, newCompatibilityList(Lock.READ_LOCK));
        lockingUnit.initWithLockCompatibilityTable(lockCompatibility);
        
        versioningUnit = VersioningUnit.instance;
        versioningUnit.init();
        
        transactions = new ConcurrentHashMap<>();
      }

    boolean checkForDeadlock(int transactionId, String key, LockType lockType)
      {
        return false;
      }

    void deadLockHandlingAtCommit(int transactionId)
      {

      }

    @Override
    public ReadResponse read(ReadRequest request)
      {
        int xid = request.getTransactionId();
        String key = request.getEncodedKey();

        Transaction transaction = transactions.get(xid);

        if (transaction == null)
          {
            return new ReadResponse(false, null);
          }
        Lock lock = Lock.READ_LOCK;
        if (checkForDeadlock(xid, key, lock))
          {
            return new ReadResponse(false, null);
          }

        lockingUnit.lock(key, lock);
        transaction.addLock(key, lock);
        String value = (String) versioningUnit.get(xid, key);
        return new ReadResponse(true, value);

      }

    @Override
    public GenericSuccessResponse write(WriteRequest request)
      {
        int xid = request.getTransactionId();
        String key = request.getEncodedKey();
        String value = request.getEncodedValue();

        Transaction transaction = transactions.get(xid);

        if (transaction == null)
          {
            return new GenericSuccessResponse(false);
          }

        Lock lock = Lock.WRITE_LOCK;
        if (checkForDeadlock(xid, key, lock))
          {
            return new GenericSuccessResponse(false);
          }

        lockingUnit.lock(key, lock);
        transaction.addLock(key, lock);
        versioningUnit.put(xid, key, value);
        return new GenericSuccessResponse(true);

      }

    @Override
    public GenericSuccessResponse begin(BeginRequest request)
      {
        int xid = request.getTransactionId();
        transactions.put(xid, new Transaction(xid));
        return new GenericSuccessResponse(true);
      }

    @Override
    public GenericSuccessResponse commit(CommitRequest request)
      {
        int xid = request.getTransactionId();

        Transaction transaction = transactions.get(xid);
        if (transaction == null)
          {
            return new GenericSuccessResponse(false);
          }

        for (Key_LockType KL : transaction.getHeldLocks())
          {

            if (KL.type == Lock.WRITE_LOCK)
              {
                if (checkForDeadlock(xid, KL.key, Lock.COMMIT_LOCK))
                  {
                    return new GenericSuccessResponse(false);
                  }

                lockingUnit.lock(KL.key, Lock.COMMIT_LOCK);
              }
          }
        versioningUnit.commit(xid);
        deadLockHandlingAtCommit(xid);
        for (Key_LockType KL : transaction.getHeldLocks())
          {
             lockingUnit.release(KL.key, KL.type); 
          }
        return new GenericSuccessResponse(true);
      }

    private ConcurrentHashMap<Integer, Transaction> transactions;

    private static enum Lock implements LockType
      {

        READ_LOCK, WRITE_LOCK, COMMIT_LOCK
      }

    private class Transaction
      {

        private int transactionId;
        private LinkedList<Key_LockType> heldLocks;

        public void addLock(String key, LockType type)
          {
            heldLocks.add(new Key_LockType(key, type));
            // TODO check redundancy
          }

        public LinkedList<Key_LockType> getHeldLocks()
          {
            return heldLocks;
          }

        public int getTransactionId()
          {
            return transactionId;
          }

        public Transaction(int transactionId)
          {
            this.transactionId = transactionId;
            heldLocks = new LinkedList<>();
          }

      }

    private class Key_LockType
      {

        String key;
        LockType type;

        public Key_LockType(String key, LockType type)
          {
            this.key = key;
            this.type = type;
          }

      }
  }
