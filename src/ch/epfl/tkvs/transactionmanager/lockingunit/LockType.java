package ch.epfl.tkvs.transactionmanager.lockingunit;

public interface LockType {

    public static enum Default implements LockType {
        READ_LOCK, WRITE_LOCK
    }

    public static enum Exclusive implements LockType {
        LOCK
    }
}
