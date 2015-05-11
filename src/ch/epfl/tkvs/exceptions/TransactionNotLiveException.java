package ch.epfl.tkvs.exceptions;

public class TransactionNotLiveException extends AbortException {

    private static final long serialVersionUID = -2834812931066424425L;

    public TransactionNotLiveException() {
        super("Transaction is not live. Aborting..");
    }

}
