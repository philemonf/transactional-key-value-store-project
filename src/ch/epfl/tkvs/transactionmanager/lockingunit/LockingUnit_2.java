package ch.epfl.tkvs.transactionmanager.lockingunit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;


/**
 * Locking Unit Singleton Call a function with LockingUnit.instance.fun(args)
 */
public enum LockingUnit_2 {
    instance;

    private LockCompatibilityTable_2 lct;
    private HashMap<Serializable, HashSet<LockType>> currentLocks = new HashMap<Serializable, HashSet<LockType>>();

    private HashMap<Serializable, HashMap<LockType, Condition>> waitingLists = new HashMap<Serializable, HashMap<LockType, Condition>>();
    private Lock internalLock = new ReentrantLock();

    private static Logger log = Logger.getLogger(LockingUnit_2.class.getName());

    /**
     * MUST be called before use to specify the default 2PL lock compatibility
     * table. To check whether lockTypeA is compatible with lockTypeB, the unit
     * will do table.areCompatible(lockTypeA, lockTypeB).
     *
     * For simplicity, please call this method before running the threads.
     */
    public void init() {
        currentLocks.clear();
        waitingLists.clear();
        lct = new LockCompatibilityTable_2(false);
    }

    /**
     * MUST be called before use to specify the default 2PL lock compatibility
     * table. To check whether lockTypeA is compatible with lockTypeB, the unit
     * will do table.areCompatible(lockTypeA, lockTypeB).
     *
     * For simplicity, please call this method before running the threads.
     */
    public void initOnlyExclusiveLock() {
        currentLocks.clear();
        waitingLists.clear();
        lct = new LockCompatibilityTable_2(true);
    }

    /**
     * MUST be called before use to specify the lock compatibility table. By
     * default, the lock compatibility table of 2PL is set. To check whether
     * lockTypeA is compatible with lockTypeB, the unit will do
     * table.areCompatible(lockTypeA, lockTypeB).
     *
     * For simplicity, please call this method before running the threads.
     * 
     * @param table
     *            the lock compatibility table - if null, use default parameter
     */
    public void initWithLockCompatibilityTable(HashMap<LockType, ArrayList<LockType>> table) {
        currentLocks.clear();
        waitingLists.clear();

        if (table == null) {
            log.warn("LockCompatibilityTable is null. Using default compatibility table.");
            lct = new LockCompatibilityTable_2(false);
        } else {
            lct = new LockCompatibilityTable_2(table);
        }
    }

    /**
     * Locks an object. Remember to init the module with the right lock
     * compatibility table.
     * 
     * @param key
     *            the key of the object to lock
     * @param lockType
     *            the lock type
     */
    public void lock(Serializable key, LockType lockType) {
        try {
            internalLock.lock();
            while (!isLockTypeCompatible(key, lockType)) {
                waitOn(key, lockType);
            }
            addToCurrentLocks(key, lockType);
        } catch (InterruptedException e) {
            // TODO: something
            log.error("Shit happens...");
        } finally {
            internalLock.unlock();
        }
    }

    /**
     * Release/Unlock an object.
     * 
     * @param key
     *            the key of the object to unlock
     * @param lockType
     *            the lock type, be careful to init the module with the right
     *            lock compatibility table.
     */
    public void release(Serializable key, LockType lockType) {
        internalLock.lock();
        removeFromCurrentLocks(key, lockType);
        signalOn(key, lockType);
        internalLock.unlock();
    }

    private HashSet<LockType> getCurrentLocks(Serializable key) {
        if (currentLocks.containsKey(key)) {
            return currentLocks.get(key);
        } else {
            return new HashSet<LockType>();
        }
    }

    private void addToCurrentLocks(Serializable key, LockType lockType) {
        if (currentLocks.containsKey(key)) {
            currentLocks.get(key).add(lockType);
        } else {
            currentLocks.put(key, new HashSet<LockType>(Arrays.asList(lockType)));
        }
        log.info("SHOULD NOT BE EMPTY: " + currentLocks.get(key));
    }

    private void removeFromCurrentLocks(Serializable key, LockType lockType) {
        HashSet<LockType> lockSet = currentLocks.get(key);
        if (lockSet != null) {
            lockSet.remove(lockType);
        }
    }

    private boolean isLockTypeCompatible(Serializable key, LockType lockType) {
        HashSet<LockType> locks = getCurrentLocks(key);
        log.info(locks);

        boolean compatible = true;
        for (LockType currLock : locks) {
            compatible = compatible && lct.areCompatible(lockType, currLock);
        }
        return compatible;
    }

    private void waitOn(Serializable key, LockType lockType) throws InterruptedException {
        HashMap<LockType, Condition> em = waitingLists.get(key);

        if (em == null) {
            em = waitingLists.put(key, new HashMap<LockType, Condition>());
        }

        if (!em.containsKey(lockType)) {
            em.put(lockType, internalLock.newCondition());
        }

        em.get(lockType).await();
    }

    private void signalOn(Serializable key, LockType lockType) {
        HashMap<LockType, Condition> em = waitingLists.get(key);

        if (em == null || !em.containsKey(lockType)) {
            return;
        }

        for (LockType lock : em.keySet()) {
            if (!lct.areCompatible(lockType, lock)) {
                em.get(lock).signal();
            }
        }

    }
}