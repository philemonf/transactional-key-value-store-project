package ch.epfl.tkvs.exceptions;

public class CommitWithoutPrepareException extends AbortException {

    public CommitWithoutPrepareException() {
        super("Commit without Prepare. Aborting..");
    }

}
