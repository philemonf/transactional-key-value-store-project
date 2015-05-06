package ch.epfl.tkvs.transactionmanager.communication.responses;

import ch.epfl.tkvs.exceptions.AbortException;

import java.io.IOException;
import java.io.Serializable;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;

import org.apache.log4j.Logger;


public class ReadResponse extends Message {

    private static final Logger log = Logger.getLogger(ReadResponse.class);
    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "read_response";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_SUCCESS)
    private boolean success;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_VALUE)
    private String encodedValue;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_EXCEPTION)
    private String exceptionMessage;

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public ReadResponse(String encodedValue) {
        this.success = true;
        this.encodedValue = encodedValue;
        this.exceptionMessage = " ";
    }

    public ReadResponse(AbortException exception) {
        this.success = false;
        this.encodedValue = "  ";
        this.exceptionMessage = exception.getMessage();
    }

    @JSONConstructor
    public ReadResponse(boolean success, String encodedValue, String exceptionMessage) {
        this.success = success;
        this.encodedValue = encodedValue;
        this.exceptionMessage = exceptionMessage;
    }

    public boolean getSuccess() {
        return success;
    }

    public Serializable getValue() {
        try {
            return Base64Utils.convertFromBase64(encodedValue);
        } catch (IOException | ClassNotFoundException e) {
            log.fatal("Cannot decode value", e);
            return null;
        }
    }

    @Override
    public String toString() {
        if (success)
            return MESSAGE_TYPE + " : " + getValue();
        else
            return MESSAGE_TYPE + " : " + exceptionMessage;
    }
}
