package ch.epfl.tkvs.transactionmanager.communication;

import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_MESSAGE_TYPE;
import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_TRANSACTION_ID;

import java.util.LinkedList;
import java.util.List;

public class TransactionTerminateMessage extends Message {
	@JSONAnnotation(key = KEY_FOR_MESSAGE_TYPE)
	public static final String MESSAGE_TYPE = "xact_terminate_message";
	
	@JSONAnnotation(key = KEY_FOR_TRANSACTION_ID)
	private int[] transactionIds;

	@JSONConstructor
	public TransactionTerminateMessage(int transactionIds[]) {
		this.transactionIds = transactionIds;
	}
	
	public TransactionTerminateMessage(List<Integer> tids) {
		this.transactionIds = new int[tids.size()];
		
		int i = 0;
		for (Integer tid: tids) {
			this.transactionIds[i++] = tid.intValue();
		}
	}
	
	public TransactionTerminateMessage(int transactionId) {
		this.transactionIds = new int [] {transactionId};
	}

	public int[] getTransactionIds() {
		return transactionIds;
	}
}
