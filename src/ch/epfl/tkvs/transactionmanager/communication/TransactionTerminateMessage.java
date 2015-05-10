package ch.epfl.tkvs.transactionmanager.communication;

import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_MESSAGE_TYPE;
import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_TRANSACTION_ID;

public class TransactionTerminateMessage extends Message {
	@JSONAnnotation(key = KEY_FOR_MESSAGE_TYPE)
	public static final String MESSAGE_TYPE = "xact_terminate_message";
	
	@JSONAnnotation(key = KEY_FOR_TRANSACTION_ID)
	private int transactionId;

	public TransactionTerminateMessage(int transactionId) {
		this.transactionId = transactionId;
	}

	public int getTransactionId() {
		return transactionId;
	}
}
