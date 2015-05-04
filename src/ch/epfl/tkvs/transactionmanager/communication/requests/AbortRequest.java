package ch.epfl.tkvs.transactionmanager.communication.requests;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;


public class AbortRequest extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "abort_request";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_TRANSACTION_ID)
    private int transactionId;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_PRIMARY_MESSAGE)
    private boolean primaryMessage;

    public int getTransactionId() {
        return transactionId;
    }

    public boolean isPrimaryMessage() {
        return primaryMessage;
    }

    public AbortRequest(int transactionId) {
        this.transactionId = transactionId;
        primaryMessage = true;
    }

    public AbortRequest(int transactionId, boolean shouldPass) {
        this.transactionId = transactionId;
        this.primaryMessage = shouldPass;
    }

}