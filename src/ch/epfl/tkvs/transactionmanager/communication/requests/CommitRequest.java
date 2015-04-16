package ch.epfl.tkvs.transactionmanager.communication.requests;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;

public class CommitRequest extends Message
  {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "commit_request";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_TRANSACTION_ID)
    private int transactionId;

    public int getTransactionId()
      {
        return transactionId;
      }

    public CommitRequest(int transactionId)
      {
        this.transactionId = transactionId;
      }

  }
