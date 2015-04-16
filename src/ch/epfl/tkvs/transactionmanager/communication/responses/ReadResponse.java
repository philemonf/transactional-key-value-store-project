package ch.epfl.tkvs.transactionmanager.communication.responses;

import java.io.IOException;
import java.io.Serializable;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;


public class ReadResponse extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "transaction_manager_response";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_SUCCESS)
    private boolean success;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_VALUE)
    private String encodedValue;

    public ReadResponse(boolean success, String encodedValue) {
        this.success = success;
        this.encodedValue = encodedValue;
    }

    public boolean getSuccess() {
        return success;
    }

    public Serializable getValue() throws IOException {
        try {
            return Base64Utils.convertFromBase64(encodedValue);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
