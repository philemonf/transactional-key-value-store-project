package ch.epfl.tkvs.transactionmanager.communication.requests;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;


public class TransactionManagerRequest extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "transaction_manager_request";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_HASH)
    private int localityHash;

    @JSONConstructor
    public TransactionManagerRequest(int hash) {
        this.localityHash = hash;
    }

    @Override
    public String toString() {
        return MESSAGE_TYPE + " : " + localityHash;
    }

    public int getLocalityHash() {
        return localityHash;
    }
}
