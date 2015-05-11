package ch.epfl.tkvs.transactionmanager.communication.requests;

import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;


/**
 * This message is sent to {@link TransactionManager} to abort a transaction
 * 
 */
public class AbortRequest extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "abort_request";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_TRANSACTION_ID)
    private int transactionId;

    @Override
    public String toString() {
        return MESSAGE_TYPE + " : t" + transactionId;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public AbortRequest(int transactionId) {
        this.transactionId = transactionId;
    }

}