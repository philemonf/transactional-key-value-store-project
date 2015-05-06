package ch.epfl.tkvs.exceptions;

public class PrepareException extends AbortException {

    public PrepareException(String string) {
        super("Cannot prepare to commit :" + string);
    }

}
