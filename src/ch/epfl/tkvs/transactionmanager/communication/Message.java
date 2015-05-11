package ch.epfl.tkvs.transactionmanager.communication;

import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter;
import ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter;


/**
 * The base class from which all the messages exchanged on the cluster inherits.
 * 
 * @see JSON2MessageConverter
 * @see Message2JSONConverter
 */
public abstract class Message {

    public Message() {

    }
}
