package ch.epfl.tkvs.exceptions;

public class CommitWithoutPrepareException extends AbortException {

    private static final long serialVersionUID = -4778753109731671047L;

    public CommitWithoutPrepareException() {
        super("Commit without Prepare. Aborting..");
    }

}
