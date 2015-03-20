package ch.epfl.tkvs.transactionmanager.lockingunit;

/**
 * The lock types available for the LockingUnit 
 * A READ_LOCK (shared lock) and a WRITE_LOCK (exclusive lock)
 */
public enum LockType {
    READ_LOCK, WRITE_LOCK
}