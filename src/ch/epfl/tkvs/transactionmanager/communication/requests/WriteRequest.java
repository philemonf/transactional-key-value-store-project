package ch.epfl.tkvs.transactionmanager.communication.requests;

import java.io.IOException;
import java.io.Serializable;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;


public class WriteRequest extends Message {
	
	@JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
	public static final String MESSAGE_TYPE = "write_request";
	
	@JSONAnnotation(key = JSONCommunication.KEY_FOR_TRANSACTION_ID)
	private int transactionId;
	
	@JSONAnnotation(key = JSONCommunication.KEY_FOR_KEY)
	private String encodedKey;
	
	@JSONAnnotation(key = JSONCommunication.KEY_FOR_VALUE)
	private String encodedValue;
	
	@JSONAnnotation(key = JSONCommunication.KEY_FOR_HASH)
	private int hash;
	
	public WriteRequest(int transactionId, Serializable key, Serializable value, int hash) throws IOException {
		this.transactionId = transactionId;
		this.encodedKey = Base64Utils.convertToBase64(key);
		this.encodedValue = Base64Utils.convertToBase64(value);
		this.hash = hash;
	}

	public int getTransactionId() {
		return transactionId;
	}

	public String getEncodedKey() {
		return encodedKey;
	}
	
	public String getEncodedValue() {
		return encodedValue;
	}

	public int getHash() {
		return hash;
	}
}
