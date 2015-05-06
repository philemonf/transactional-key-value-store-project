package ch.epfl.tkvs.exceptions;

public class RemoteTMException extends AbortException {

    public RemoteTMException(String string) {
        super("Remote TM caused exception : " + string);
    }

    public RemoteTMException() {
        super("Invalid or no response from Remote TM");
    }
}
