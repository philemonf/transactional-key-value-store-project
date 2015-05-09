package ch.epfl.tkvs.transactionmanager.communication.requests;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;


public class BeginRequest extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "begin_request";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_TRANSACTION_ID)
    private int transactionId;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_PRIMARY_MSG)
    private boolean primary;

    @Override
    public String toString() {
        return MESSAGE_TYPE + " : t" + transactionId + "  primary?" + primary;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public boolean isPrimary() {
        return primary;
    }

    @JSONConstructor
    public BeginRequest(int transactionId, boolean primary) {
        this.transactionId = transactionId;
        this.primary = primary;
    }

    public BeginRequest(int transactionId) {
        this.transactionId = transactionId;
        this.primary = true;
    }

}
