package ch.epfl.tkvs.transactionmanager.communication.requests;

import ch.epfl.tkvs.test.userclient.UserClient;
import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;


/**
 * This message is sent to {@link TransactionManager} to commit a transaction. This message is sent by the primary
 * {@link TransactionManager} to other {@link TransactionManager}s upon successfully completing prepare phase during
 * 2-Phase Commit This is NOT the message send by the {@link UserClient} to tell the primary {@link TransactionManager}
 * to initiate 2-Phase commit. See {@link TryCommitRequest}
 */
public class CommitRequest extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "commit_request";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_TRANSACTION_ID)
    private int transactionId;

    public int getTransactionId() {
        return transactionId;
    }

    @Override
    public String toString() {
        return MESSAGE_TYPE + ": t" + transactionId;
    }

    @JSONConstructor
    public CommitRequest(int transactionId) {
        this.transactionId = transactionId;
    }

}
