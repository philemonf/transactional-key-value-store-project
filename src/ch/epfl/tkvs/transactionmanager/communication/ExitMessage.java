package ch.epfl.tkvs.transactionmanager.communication;

/**
 * This message is sent to turn the whole system down in a proper manner.
 */
public class ExitMessage extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "exit_message";
}
