package ch.epfl.tkvs.exceptions;

public class DeadlockException extends AbortException {

    public DeadlockException() {
        super("Deadlock");
    }

}
