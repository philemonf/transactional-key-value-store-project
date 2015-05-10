package ch.epfl.tkvs.transactionmanager.communication.requests;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;

/**
 * Used for GC in MVTO to determine what is the transaction with the smallest timestamp that is alive.
 */
public class MinAliveTransactionRequest extends Message {
	
	@JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "min_alive_xact_request";
	
}
