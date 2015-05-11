package ch.epfl.tkvs.exceptions;

public class TransactionAlreadyExistsException extends AbortException {

    private static final long serialVersionUID = -4187118277093444317L;

    public TransactionAlreadyExistsException() {
        super("Transaction with given id already exists in Transaction Manager. Aborting..");
    }

}
