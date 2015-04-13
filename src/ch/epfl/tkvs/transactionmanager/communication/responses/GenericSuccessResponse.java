package ch.epfl.tkvs.transactionmanager.communication.responses;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;


public class GenericSuccessResponse extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_SUCCESS)
    private final boolean success = true;
}
