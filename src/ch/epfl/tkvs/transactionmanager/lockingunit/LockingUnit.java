package ch.epfl.tkvs.transactionmanager.lockingunit;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Locking Unit Singleton Call a function with LockingUnit.instance.fun(args)
 */
public enum LockingUnit{
    instance;

    private LockCompatibilityTable lockCompatibilityTable = defaultCompatibilityTable();

    private Map<Serializable, EnumSet> currentLockTypes = new HashMap<Serializable, EnumSet>();
    private Map<Serializable, EnumMap<? extends Enum, Condition>> waitingLists = new HashMap<Serializable, EnumMap<? extends Enum, Condition>>();
    private Lock internalLock = new ReentrantLock();

    private static Logger log = Logger.getLogger(LockingUnit.class.getName());

    /**
     * MUST be called before use to specify the lock compatibility table.
     * By default, the lock compatibility table of 2PL is set.
     * To check whether lockTypeA is compatible with lockTypeB, the unit
     * will do table.areCompatible(lockTypeA, lockTypeB).
     *
     * For simplicity, please call this method before running the threads.
     * @param table the lock compatibility table - if null, use default parameter
     */
    public void initWithLockCompatibilityTable(LockCompatibilityTable table) {
        unlockAll();
        currentLockTypes.clear();
        waitingLists.clear();

        if (table == null) {
            lockCompatibilityTable = defaultCompatibilityTable();
        } else {
            lockCompatibilityTable = table;
        }
    }

    /**
     * Lock an object.
     * @param key the key of the object to lock
     * @param lockType the lock type, be careful to init the module with the right lock compatibility table
     */
    public <E extends Enum<E>> void lock(Serializable key, E lockType) {
        internalLock.lock();

        try {
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
     * @param key the key of the object to unlock
     * @param lockType the lock type, be careful to init the module with the right lock compatibility table.
     */
    public <E extends Enum<E>> void release(Serializable key, E lockType) {
        internalLock.lock();
        try {
            removeFromCurrentLocks(key, lockType);
            signalOn(key, lockType);
        } finally {
            internalLock.unlock();
        }
    }

    /**
     * Release all the locks currently locked.
     */
    public void unlockAll() {
        internalLock.lock();
        internalLock.unlock();
    }

    /**
     * The default lock type of the unit.
     * READ_LOCK is only compatible with itself.
     * All other combinations are not compatible.
     */
    public static enum DefaultLockType {
        READ_LOCK, WRITE_LOCK
    }

    private static LockCompatibilityTable defaultCompatibilityTable() {
        return new LockCompatibilityTable() {
            @Override
            public <E extends Enum<E>> boolean areCompatible(E lock1, E lock2) {
                log.info("Test compatibility between " + lock1 + " and " + lock2);
                return lock1.equals(DefaultLockType.READ_LOCK) && lock2.equals(DefaultLockType.READ_LOCK);
            }
        };
    }

    private Set<? extends Enum> getCurrentLocks(Serializable key, Class<? extends Enum> lockTypes) {
        if (currentLockTypes.containsKey(key)) {
            return currentLockTypes.get(key);
        } else {
            return EnumSet.noneOf(lockTypes);
        }
    }

    private <E extends Enum<E>> void addToCurrentLocks(Serializable key, E lockType) {

        if (currentLockTypes.containsKey(key)) {
            EnumSet lockSet = currentLockTypes.get(key);
            lockSet.add(lockType);
        } else {
            EnumSet lockSet = EnumSet.noneOf(lockType.getClass());
            lockSet.add(lockType);
            currentLockTypes.put(key, lockSet);
        }

        log.info("SHOULD NOT BE EMPTY: " + getCurrentLocks(key, lockType.getClass()));
    }

    private <E extends Enum<E>> void removeFromCurrentLocks(Serializable key, E lockType) {

        if (currentLockTypes.containsKey(key)) {
            EnumSet lockSet = currentLockTypes.get(key);

            if (lockSet.contains(lockType)) {
                lockSet.remove(lockType);
            }
        }
    }

    private <E extends Enum<E>>  boolean isLockTypeCompatible(Serializable key, E lockType) {
        Set<? extends Enum> currentLocksOnKey = getCurrentLocks(key, lockType.getClass());
        log.info(currentLocksOnKey);

        boolean compatible = true;
        for (Enum currLock : currentLocksOnKey) {
            compatible = compatible && lockCompatibilityTable.areCompatible(lockType, currLock);
        }

        return compatible;
    }

    private <E extends Enum<E>> void waitOn(Serializable key, E lockType) throws InterruptedException {
        if (!waitingLists.containsKey(key)) {
            waitingLists.put(key, new EnumMap(lockType.getClass()));
        }

        if (!waitingLists.get(key).containsKey(lockType)) {
            EnumMap waitingList = waitingLists.get(key);
            waitingList.put(lockType, internalLock.newCondition());
        }

        waitingLists.get(key).get(lockType).await();
    }

    private <E extends Enum<E>> void signalOn(Serializable key, E lockType) {
        if (!waitingLists.containsKey(key)) {
            return;
        }

        if (!waitingLists.get(key).containsKey(lockType)) {
            return;
        }

        for (Enum otherLockType : waitingLists.get(key).keySet()) {
            if (!lockCompatibilityTable.areCompatible(lockType, otherLockType)) {
                waitingLists.get(key).get(otherLockType).signal();
            }
        }

    }
}