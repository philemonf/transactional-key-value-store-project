package ch.epfl.tkvs.exceptions;

public class TransactionAlreadyExistsException extends AbortException {

    public TransactionAlreadyExistsException() {
        super("Transaction with given id already exists in Transaction Manager. Aborting..");
    }

}
