package ch.epfl.tkvs.transactionmanager.lockingunit;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;


/**
 * Locking Unit Singleton Call a function with LockingUnit.instance.fun(args)
 */
public enum LockingUnit {
    instance;

    private LockCompatibilityTable lct;
    private Map<Serializable, List<LockType>> locks = new HashMap<Serializable, List<LockType>>();

    private Map<Serializable, HashMap<LockType, Condition>> waitingLists = new HashMap<Serializable, HashMap<LockType, Condition>>();
    private Lock internalLock = new ReentrantLock();

    private static Logger log = Logger.getLogger(LockingUnit.class.getName());

    /**
     * MUST be called before use to specify the default 2PL lock compatibility table. To check whether lockTypeA is
     * compatible with lockTypeB, the unit will do table.areCompatible(lockTypeA, lockTypeB).
     *
     * For simplicity, please call this method before running the threads.
     */
    public void init() {
        locks = new HashMap<Serializable, List<LockType>>();
        waitingLists = new HashMap<Serializable, HashMap<LockType, Condition>>();
        lct = new LockCompatibilityTable(false);
    }

    /**
     * MUST be called before use to specify the default 2PL lock compatibility table. To check whether lockTypeA is
     * compatible with lockTypeB, the unit will do table.areCompatible(lockTypeA, lockTypeB).
     *
     * For simplicity, please call this method before running the threads.
     */
    public void initOnlyExclusiveLock() {
        locks = new HashMap<Serializable, List<LockType>>();
        waitingLists = new HashMap<Serializable, HashMap<LockType, Condition>>();
        lct = new LockCompatibilityTable(true);
    }

    /**
     * MUST be called before use to specify the lock compatibility table. By default, the lock compatibility table of
     * 2PL is set. To check whether lockTypeA is compatible with lockTypeB, the unit will do
     * table.areCompatible(lockTypeA, lockTypeB).
     *
     * For simplicity, please call this method before running the threads.
     * 
     * @param table the lock compatibility table - if null, use default parameter
     */
    public void initWithLockCompatibilityTable(Map<LockType, List<LockType>> table) {
        locks = new HashMap<Serializable, List<LockType>>();
        waitingLists = new HashMap<Serializable, HashMap<LockType, Condition>>();

        if (table == null) {
            log.warn("LockCompatibilityTable is null. Using default compatibility table.");
            lct = new LockCompatibilityTable(false);
        } else {
            lct = new LockCompatibilityTable(table);
        }
    }

    /**
     * Locks an object. Remember to init the module with the right lock compatibility table.
     * 
     * @param key the key of the object to lock
     * @param lockType the lock type
     */
    public void lock(Serializable key, LockType lockType) {
        try {
            internalLock.lock();
            while (!canLock(key, lockType)) {
                waitOn(key, lockType);
            }
            addLock(key, lockType);
        } catch (InterruptedException e) {
            // TODO: something
            log.error("Shit happens...");
        } finally {
            internalLock.unlock();
        }
    }

    /**
     * Promotes a lock on an object atomically.
     *
     * @param key the key of the object associated with the lock
     * @param oldTypes the lock types to promote
     * @param newType the new lock type
     */
    public <T extends LockType> void promote(Serializable key, List<T> oldTypes, LockType newType) {
        try {
            internalLock.lock();
            if (locks.containsKey(key)) {
                // Copy the list of current locks and remove the old types from the copy
                List<LockType> theLocks = allLocksExcept(key, oldTypes);
                
                if (!theLocks.isEmpty()) {
                    while (!lct.areCompatible(newType, theLocks)) {
                        waitOn(key, newType);
                        
                        // Recompute the copy of the locks to avoid bug
                        theLocks = allLocksExcept(key, oldTypes);
                    }
                }
                
                addLock(key, newType);

                for (LockType oldType : oldTypes) {
                    removeLock(key, oldType);
                }
            }
        } catch (InterruptedException e) {
            // TODO: something
            log.error("Shit happens...");
        } finally {
            internalLock.unlock();
        }
    }
    
    private <T extends LockType> List<LockType> allLocksExcept(Serializable key, List<T> locksToExclude) {
    	List<LockType> theLocks = new LinkedList<LockType>(locks.get(key));
        
        for (LockType lock : locksToExclude) {
        	theLocks.remove(lock);
        }
        
        return theLocks;
    }

    /**
     * Release/Unlock an object.
     * 
     * @param key the key of the object to unlock
     * @param lockType the lock type, be careful to init the module with the right lock compatibility table.
     */
    public void release(Serializable key, LockType lockType) {
        internalLock.lock();
        removeLock(key, lockType);
        signalOn(key, lockType);
        internalLock.unlock();
    }

    private boolean canLock(Serializable key, LockType lockType) {
        if (locks.containsKey(key))
            return lct.areCompatible(lockType, locks.get(key));
        return true;
    }

    private void addLock(Serializable key, LockType lockType) {
        if (locks.containsKey(key)) {
            locks.get(key).add(lockType);
        } else {
            locks.put(key, new LinkedList<LockType>(Arrays.asList(lockType)));
        }
    }

    private void removeLock(Serializable key, LockType lockType) {
        if (locks.containsKey(key)) {
            locks.get(key).remove(lockType);
        }
        
        if (locks.get(key).isEmpty()) {
            locks.remove(key);
        }
    }

    private void waitOn(Serializable key, LockType lockType) throws InterruptedException {
        HashMap<LockType, Condition> em = waitingLists.get(key);

        if (em == null) {
            waitingLists.put(key, new HashMap<LockType, Condition>());
            em = waitingLists.get(key);
        }

        if (!em.containsKey(lockType)) {
            em.put(lockType, internalLock.newCondition());
        }

        em.get(lockType).await();
    }

    private void signalOn(Serializable key, LockType lockType) {
        HashMap<LockType, Condition> em = waitingLists.get(key);

        if (em == null) {
            return;
        }

        for (LockType otherLockType : em.keySet()) {
            if (!lct.areCompatible(lockType, otherLockType)) {
                em.get(otherLockType).signal();
            }
        }

    }
}