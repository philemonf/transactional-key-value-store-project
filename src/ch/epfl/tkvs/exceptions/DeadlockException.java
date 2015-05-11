package ch.epfl.tkvs.exceptions;

public class DeadlockException extends AbortException {

    private static final long serialVersionUID = 981768773388469784L;

    public DeadlockException() {
        super("Deadlock. Aborting..");
    }

}
