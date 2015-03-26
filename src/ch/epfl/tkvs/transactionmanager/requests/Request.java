package ch.epfl.tkvs.transactionmanager.requests;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.codec.binary.Base64;
import org.codehaus.jettison.json.JSONObject;


public abstract class Request {
	
	public static final String JSON_KEY_FOR_REQUEST_TYPE = "request_type";
	public static final String JSON_KEY_FOR_KEY = "key";
	public static final String JSON_KEY_FOR_VALUE = "value";
	public static final String JSON_KEY_FOR_HASH = "hash";
	
	public static class InvalidRequestException extends Exception {
		public InvalidRequestException(String message) {
			super(message);
		}
		
		public InvalidRequestException(Exception parent) {
			super(parent);
		}
	}
	
	abstract public JSONObject toJSON() throws InvalidRequestException;
	
	public static Serializable convertFromBase64(String base64) throws IOException, ClassNotFoundException {
		byte[] buf = Base64.decodeBase64(base64);
		ByteArrayInputStream bis = new ByteArrayInputStream(buf);
		ObjectInputStream ois = new ObjectInputStream(bis);
		
		Serializable object = (Serializable) ois.readObject();
		return object;
	}
	
	public static String convertToBase64(Serializable data) throws IOException {
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		
		oos.writeObject(data);
		
		byte[] bytes = bos.toByteArray();
		bos.close();
		
		return Base64.encodeBase64String(bytes);
	}
}
