package ch.epfl.tkvs.exceptions;

public class TimestampOrderingException extends AbortException {

    public TimestampOrderingException(String string) {
        super(string + "Aborting..");
    }

}
