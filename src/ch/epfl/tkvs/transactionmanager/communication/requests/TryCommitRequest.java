package ch.epfl.tkvs.transactionmanager.communication.requests;

import ch.epfl.tkvs.test.userclient.UserClient;
import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;


/**
 * This message is sent by the {@link UserClient } to the primary {@link TransactionManager} to initiate 2-Phase Commit.
 * 
 */
public class TryCommitRequest extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "try_commit_request";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_TRANSACTION_ID)
    private int transactionId;

    public int getTransactionId() {
        return transactionId;
    }

    @Override
    public String toString() {
        return MESSAGE_TYPE + " : t" + transactionId;
    }

    @JSONConstructor
    public TryCommitRequest(int transactionId) {
        this.transactionId = transactionId;

    }

}
