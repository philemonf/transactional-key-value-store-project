package ch.epfl.tkvs.transactionmanager.communication.responses;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;


public class GenericSuccessResponse extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_SUCCESS)
    private boolean success;

    public GenericSuccessResponse(boolean success)
      {
        this.success = success;
      }

    public boolean isSuccess()
      {
        return success;
      }

    }
