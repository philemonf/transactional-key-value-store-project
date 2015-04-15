package ch.epfl.tkvs.transactionmanager.lockingunit;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Locking Unit Singleton Call a function with LockingUnit.instance.fun(args)
 */
public enum LockingUnit{
    instance;

    private LockCompatibilityTable lockCompatibilityTable = defaultCompatibilityTable();

    private Map<Serializable, EnumSet> currentLockTypes = new HashMap<Serializable, EnumSet>();
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

        internalLock.unlock();
    }

    /**
     * Release/Unlock an object.
     * @param key the key of the object to unlock
     * @param lockType the lock type, be careful to init the module with the right lock compatibility table.
     */
    public <E extends Enum<E>> void release(Serializable key, E lockType) {
        internalLock.lock();
        log.info("A key has been unlocked !");
        internalLock.unlock();
    }

    /**
     * Release all the locks currently locked.
     */
    public void unlockAll() {
        internalLock.lock();
        log.info("All keys have been unlocked !");
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
    }

    private <E extends Enum<E>>  boolean isLockTypeCompatible(Serializable key, E lockType) {
        Set<? extends Enum> currentLocksOnKey = getCurrentLocks(key, lockType.getClass());

        boolean compatible = true;
        for (Enum currLock : currentLocksOnKey) {
            compatible = compatible && lockCompatibilityTable.areCompatible(lockType, currLock);
        }

        return compatible;
    }

}