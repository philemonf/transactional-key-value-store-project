package ch.epfl.tkvs.exceptions;

public class TimestampOrderingException extends AbortException {

    private static final long serialVersionUID = 5940943570067027374L;

    public TimestampOrderingException(String string) {
        super(string + "Aborting..");
    }

}
