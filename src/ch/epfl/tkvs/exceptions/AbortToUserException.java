package ch.epfl.tkvs.exceptions;

public class AbortToUserException extends AbortException {

    private static final long serialVersionUID = -7601973276505000066L;

    public AbortToUserException(String string) {
        super(string);
    }

}
