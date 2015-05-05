package ch.epfl.tkvs.transactionmanager.communication.responses;

import java.io.IOException;
import java.io.Serializable;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
import org.apache.log4j.Logger;


public class ReadResponse extends Message {

    private static Logger log = Logger.getLogger(ReadResponse.class);
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

    public Serializable getValue() {
        if (encodedValue == null)
            return null;
        try {
            return Base64Utils.convertFromBase64(encodedValue);
        } catch (IOException | ClassNotFoundException e) {
            log.fatal("Cannot decode value", e);
            return null;
        }
    }
}
