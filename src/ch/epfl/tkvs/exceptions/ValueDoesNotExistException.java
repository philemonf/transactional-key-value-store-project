package ch.epfl.tkvs.exceptions;

public class ValueDoesNotExistException extends AbortException {

    private static final long serialVersionUID = -5734800030744598167L;

    public ValueDoesNotExistException() {
        super("The requested value does not exist. Aborting..");
    }

}
