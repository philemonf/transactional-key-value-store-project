package ch.epfl.tkvs.transactionmanager.lockingunit;

/**
 * A lock compatibility table that one should provide the LockingUnit with.
 */
public interface LockCompatibilityTable {

    /**
     * Returns true iff lock1 is compatible with lock2.
     * 
     * @param lock1
     *            the first lock type
     * @param lock2
     *            the second lock type
     * @return true iff lock1 is compatible with lock2.
     */
    <E extends Enum<E>> boolean areCompatible(E lock1, E lock2);
}