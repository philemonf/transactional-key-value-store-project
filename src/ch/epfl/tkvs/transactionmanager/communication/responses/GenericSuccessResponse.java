package ch.epfl.tkvs.transactionmanager.communication.responses;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;


public class GenericSuccessResponse extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "generic_success_response";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_SUCCESS)
    private boolean success;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_EXCEPTION)
    private String exceptionMessage;

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public GenericSuccessResponse(AbortException exception) {

        this.success = false;
        this.exceptionMessage = exception.getMessage();
    }

    public GenericSuccessResponse() {
        this.success = true;
        exceptionMessage = " ";
    }

    @JSONConstructor
    public GenericSuccessResponse(boolean success, String exceptionMessage) {
        this.success = success;
        this.exceptionMessage = exceptionMessage;
    }

    @Override
    public String toString() {
        if (success)
            return MESSAGE_TYPE;
        else
            return MESSAGE_TYPE + " : " + exceptionMessage;
    }

    public boolean getSuccess() {
        return success;
    }

}
