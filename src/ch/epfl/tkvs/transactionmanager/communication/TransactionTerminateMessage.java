package ch.epfl.tkvs.transactionmanager.communication;

import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_MESSAGE_TYPE;
import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_TRANSACTION_ID;

import java.util.LinkedList;

public class TransactionTerminateMessage extends Message {
	@JSONAnnotation(key = KEY_FOR_MESSAGE_TYPE)
	public static final String MESSAGE_TYPE = "xact_terminate_message";
	
	@JSONAnnotation(key = KEY_FOR_TRANSACTION_ID)
	private LinkedList<Integer> transactionIds;
	
	@JSONConstructor
	public TransactionTerminateMessage(LinkedList<Integer> tids) {
		this.transactionIds = tids;
	}
	
	public TransactionTerminateMessage(int transactionId) {
		this.transactionIds = new LinkedList<Integer>();
		this.transactionIds.add(transactionId);
	}

	public LinkedList<Integer> getTransactionIds() {
		return transactionIds;
	}
}
