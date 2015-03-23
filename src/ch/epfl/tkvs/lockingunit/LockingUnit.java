package ch.epfl.tkvs.lockingunit;



/**
 * Locking Unit Singleton Call a function with LockingUnit.instance.fun(args)
 */
public enum LockingUnit {
    instance;

    /**
     * Acquire a lock of type lockType on the given key
     * 
     * @return true if the lock is successfully acquired, false if already
     *         locked
     */
    public boolean lock(String key, LockType lockType) {
        System.out.println("A key has been locked !");
        return true;
    }

    /**
     * Release/Unlock a lock of type lockType of the given key
     * 
     * @return Returns true if the lock is successfully released.
     */
    public boolean release(String key, LockType lockType) {
        System.out.println("A key has been unlocked !");
        return true;
    }

    /**
     * Release all the locks contained in the lock tables
     */
    public void unlockAll() {
        System.out.println("All keys have been unlocked !");
    }

}