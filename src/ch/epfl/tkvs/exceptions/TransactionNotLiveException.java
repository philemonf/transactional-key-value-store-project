package ch.epfl.tkvs.exceptions;

public class TransactionNotLiveException extends AbortException {

    public TransactionNotLiveException() {
        super("Transaction is not live");
    }

}
