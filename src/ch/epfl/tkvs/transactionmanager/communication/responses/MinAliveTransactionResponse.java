package ch.epfl.tkvs.transactionmanager.communication.responses;

import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_MESSAGE_TYPE;
import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_TRANSACTION_ID;
import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;

/**
 * Used for GC in MVTO to determine what is the transaction with the smallest timestamp that is alive.
 */
public class MinAliveTransactionResponse extends Message {
	@JSONAnnotation(key = KEY_FOR_MESSAGE_TYPE)
	public static final String MESSAGE_TYPE = "min_alive_xact_response";
	
	@JSONAnnotation(key = KEY_FOR_TRANSACTION_ID)
	private int transactionId;

	@JSONConstructor
	public MinAliveTransactionResponse(int transactionId) {
		this.transactionId = transactionId;
	}

	public int getTransactionId() {
		return transactionId;
	}
}
