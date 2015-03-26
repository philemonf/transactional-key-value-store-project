package ch.epfl.tkvs.transactionmanager.requests;

import java.io.IOException;
import java.io.Serializable;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class WriteRequest extends Request {
	private int hash;
	private String keyBase64;
	private String valueBase64;
	
	public static final String TYPE = "write";
	
	public WriteRequest(JSONObject json) throws InvalidRequestException {
		
		if (json == null) {
			throw new InvalidRequestException("The JSON representation is null.");
		}
		
		try {
			
			String requestType = json.getString(JSON_KEY_FOR_REQUEST_TYPE);
			if (!requestType.equals(TYPE)) {
				throw new InvalidRequestException("Parsing a JSON request from the wrong type: " + requestType);
			}
			
			this.keyBase64 = json.getString(JSON_KEY_FOR_KEY);
			this.valueBase64 = json.getString(JSON_KEY_FOR_VALUE);
			this.hash = json.getInt(JSON_KEY_FOR_HASH);
			
		} catch (JSONException e) {
			throw new InvalidRequestException(e);
		}
	}

	public WriteRequest(Serializable key, Serializable value, int hash) throws IOException {
		this.keyBase64 = Request.convertToBase64(key);
		this.valueBase64 = Request.convertToBase64(value);
		this.hash = hash;
	}

	public String getKeyBase64() {
		return keyBase64;
	}
	
	public String getValueKeyBase64() {
		return valueBase64;
	}
	
	public Serializable getKey() throws IOException, ClassNotFoundException {
		return convertFromBase64(keyBase64);
	}
	
	public Serializable getValue() throws IOException, ClassNotFoundException {
		return convertFromBase64(valueBase64);
	}

	public int getHash() {
		return hash;
	}

	@Override
	public JSONObject toJSON() throws InvalidRequestException {
		JSONObject json = new JSONObject();
		
		try {
			json.put(JSON_KEY_FOR_REQUEST_TYPE, "read");
			json.put(JSON_KEY_FOR_KEY, keyBase64);
			json.put(JSON_KEY_FOR_VALUE, valueBase64);
			json.put(JSON_KEY_FOR_HASH, hash);
		} catch (JSONException e) {
			throw new InvalidRequestException(e);
		}
		
		return json;
	}
}
