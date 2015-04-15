package ch.epfl.tkvs.transactionmanager.lockingunit;

import java.io.Serializable;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;


/**
 * Locking Unit Singleton Call a function with LockingUnit.instance.fun(args)
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public enum LockingUnit {
    instance;

    private LockCompatibilityTable lockCompatibilityTable;
    private Map<Serializable, EnumSet> currentLockTypes = new HashMap<Serializable, EnumSet>();

    private Map<Serializable, EnumMap<? extends Enum, Condition>> waitingLists = new HashMap<Serializable, EnumMap<? extends Enum, Condition>>();
    private Lock internalLock = new ReentrantLock();

    private static Logger log = Logger.getLogger(LockingUnit.class.getName());

    /**
     * The default lock type of the unit. READ_LOCK is only compatible with
     * itself. All other combinations are not compatible.
     */
    public static enum DefaultLockType {
        READ_LOCK, WRITE_LOCK
    }

    public static enum ExclusiveLockType {
        LOCK
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

    /**
     * MUST be called before use to specify the default 2PL lock compatibility
     * table. To check whether lockTypeA is compatible with lockTypeB, the unit
     * will do table.areCompatible(lockTypeA, lockTypeB).
     *
     * For simplicity, please call this method before running the threads.
     */
    public void init() {
        currentLockTypes.clear();
        waitingLists.clear();
        lockCompatibilityTable = defaultCompatibilityTable();
    }

    /**
     * MUST be called before use to specify the default 2PL lock compatibility
     * table. To check whether lockTypeA is compatible with lockTypeB, the unit
     * will do table.areCompatible(lockTypeA, lockTypeB).
     *
     * For simplicity, please call this method before running the threads.
     */
    public void initOnlyExclusiveLock() {
        currentLockTypes.clear();
        waitingLists.clear();
        lockCompatibilityTable = new LockCompatibilityTable() {

            @Override
            public <E extends Enum<E>> boolean areCompatible(E lock1, E lock2) {
                return false; // if there is a lock there is no compatibility.
            }
        };
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
    public void initWithLockCompatibilityTable(LockCompatibilityTable table) {
        currentLockTypes.clear();
        waitingLists.clear();

        if (table == null) {
            log.warn("LockCompatibilityTable is null. Using default compatibility table.");
            lockCompatibilityTable = defaultCompatibilityTable();
        } else {
            lockCompatibilityTable = table;
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
    public <E extends Enum<E>> void lock(Serializable key, E lockType) {
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
    public <E extends Enum<E>> void release(Serializable key, E lockType) {
        internalLock.lock();
        removeFromCurrentLocks(key, lockType);
        signalOn(key, lockType);
        internalLock.unlock();
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
            currentLockTypes.get(key).add(lockType);
        } else {
            currentLockTypes.put(key, EnumSet.of(lockType));
        }
        log.info("SHOULD NOT BE EMPTY: " + currentLockTypes.get(key));
    }

    private <E extends Enum<E>> void removeFromCurrentLocks(Serializable key, E lockType) {
        EnumSet lockSet = currentLockTypes.get(key);
        if (lockSet != null && lockSet.contains(lockType)) {
            lockSet.remove(lockType);
        }
    }

    private <E extends Enum<E>> boolean isLockTypeCompatible(Serializable key, E lockType) {
        Set<? extends Enum> currentLocksOnKey = getCurrentLocks(key, lockType.getClass());
        log.info(currentLocksOnKey);

        boolean compatible = true;
        for (Enum currLock : currentLocksOnKey) {
            compatible = compatible && lockCompatibilityTable.areCompatible(lockType, currLock);
        }
        return compatible;
    }

    private <E extends Enum<E>> void waitOn(Serializable key, E lockType) throws InterruptedException {
        EnumMap<? extends Enum, Condition> em = waitingLists.get(key);

        if (em == null) {
            em = waitingLists.put(key, new EnumMap(lockType.getClass()));
        }

        if (!em.containsKey(lockType)) {
            EnumMap waitList = waitingLists.get(key);
            waitList.put(lockType, internalLock.newCondition());
        }

        em.get(lockType).await();
    }

    private <E extends Enum<E>> void signalOn(Serializable key, E lockType) {
        EnumMap<? extends Enum, Condition> em = waitingLists.get(key);

        if (em == null || !em.containsKey(lockType)) {
            return;
        }

        for (Enum otherLockType : em.keySet()) {
            if (!lockCompatibilityTable.areCompatible(lockType, otherLockType)) {
                em.get(otherLockType).signal();
            }
        }

    }
}