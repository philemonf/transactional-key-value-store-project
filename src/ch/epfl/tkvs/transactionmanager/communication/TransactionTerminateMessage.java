package ch.epfl.tkvs.transactionmanager.communication;

import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_MESSAGE_TYPE;
import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_TRANSACTION_ID;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;


public class TransactionTerminateMessage extends Message {

    @JSONAnnotation(key = KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "xact_terminate_message";

    @JSONAnnotation(key = KEY_FOR_TRANSACTION_ID)
    private String encodedTids;

    @JSONConstructor
    public TransactionTerminateMessage(LinkedList<Integer> tids) throws IOException {
    	
        encodedTids = Base64Utils.convertToBase64(tids);
    }

    public TransactionTerminateMessage(int transactionId) throws IOException {
        this(new LinkedList<Integer>(Arrays.asList(transactionId)));
    }

    public LinkedList<Integer> getTransactionIds() {
        try {
            return (LinkedList<Integer>) Base64Utils.convertFromBase64(encodedTids);
        } catch (Exception e) {
            return new LinkedList<Integer>();
        }
    }
}
