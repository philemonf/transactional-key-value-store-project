package ch.epfl.tkvs.transactionmanager.communication;

public class DeadlockMessage extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "deadlock_message";

}
