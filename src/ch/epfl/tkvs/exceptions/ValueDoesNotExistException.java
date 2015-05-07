package ch.epfl.tkvs.exceptions;

public class ValueDoesNotExistException extends AbortException {

    public ValueDoesNotExistException() {
        super("The requested value does not exist. Aborting..");
    }

}
