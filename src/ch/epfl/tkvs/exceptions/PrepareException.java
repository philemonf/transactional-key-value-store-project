package ch.epfl.tkvs.exceptions;

public class PrepareException extends AbortException {

    private static final long serialVersionUID = -570334764600843941L;

    public PrepareException(String string) {
        super("Cannot prepare to commit :" + string);
    }

}
