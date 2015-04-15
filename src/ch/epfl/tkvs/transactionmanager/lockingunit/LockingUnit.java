package ch.epfl.tkvs.transactionmanager.lockingunit;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Locking Unit Singleton Call a function with LockingUnit.instance.fun(args)
 */
public enum LockingUnit{
    instance;

    private LockCompatibilityTable<? extends Enum> lockCompatibilityTable = defaultCompatibilityTable;

    private static Logger log = Logger.getLogger(LockingUnit.class.getName());

    /**
     * MUST be called before use to specify the lock compatibility table.
     * By default, the lock compatibility table of 2PL is set.
     * To check whether lockTypeA is compatible with lockTypeB, the unit
     * will do table.areCompatible(lockTypeA, lockTypeB).
     * @param table the lock compatibility table
     */
    public void initWithLockCompatibilityTable(LockCompatibilityTable<? extends Enum> table) {
        lockCompatibilityTable = table;
    }

    /**
     * Lock an object.
     * @param key the key of the object to lock
     * @param lockType the lock type, be careful to init the module with the right lock compatibility table
     * @return true if the lock is successfully acquired, false if already
     *         locked
     */
    public <E extends Enum<E>> boolean lock(Serializable key, E lockType) {
        log.info("A key has been locked !");
        return true;
    }

    /**
     * Release/Unlock an object.
     * @param key the key of the object to unlock
     * @param lockType the lock type, be careful to init the module with the right lock compatibility table.
     * @return Returns true if the lock is successfully released.
     */
    public <E extends Enum<E>> boolean release(Serializable key, E lockType) {
        log.info("A key has been unlocked !");
        return true;
    }

    /**
     * Release all the locks currently locked.
     */
    public void unlockAll() {
        log.info("All keys have been unlocked !");
    }

    /**
     * The default lock type of the unit.
     * READ_LOCK is only compatible with itself.
     * All other combinations are not compatible.
     */
    public static enum DefaultLockType {
        READ_LOCK, WRITE_LOCK
    }

    private static LockCompatibilityTable<DefaultLockType> defaultCompatibilityTable = new LockCompatibilityTable<DefaultLockType>() {
        @Override
        public boolean areCompatible(DefaultLockType lock1, DefaultLockType lock2) {
            return lock1 == DefaultLockType.READ_LOCK && lock2 == DefaultLockType.READ_LOCK;
        }
    };

}