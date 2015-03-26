package ch.epfl.tkvs.transactionmanager.requests;

import java.io.IOException;
import java.io.Serializable;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ReadRequest extends Request {
	private String keyBase64;
	private int hash;
	
	
	public static final String TYPE = "read";
	
	public ReadRequest(JSONObject json) throws InvalidRequestException {
		
		if (json == null) {
			throw new InvalidRequestException("The JSON representation is null.");
		}
		
		try {
			
			if (!json.getString(JSON_KEY_FOR_REQUEST_TYPE).equals(TYPE)) {
				throw new InvalidRequestException("Parsing a JSON request from the wrong type");
			}
			
			this.keyBase64 = json.getString(JSON_KEY_FOR_KEY);
			this.hash = json.getInt(JSON_KEY_FOR_HASH);
			
		} catch (JSONException e) {
			throw new InvalidRequestException(e);
		}
	}

	public ReadRequest(Serializable key, int hash) throws IOException {
		this.keyBase64 = Request.convertToBase64(key);
		this.hash = hash;
	}
	
	

	public String getKeyBase64() {
		return keyBase64;
	}
	
	public Serializable getKey() throws IOException, ClassNotFoundException {
		return convertFromBase64(keyBase64);
	}

	public int getHash() {
		return hash;
	}

	@Override
	public JSONObject toJSON() throws InvalidRequestException {
		JSONObject json = new JSONObject();
		
		try {
			json.put(JSON_KEY_FOR_REQUEST_TYPE, "read");
			json.put(JSON_KEY_FOR_HASH, hash);
			json.put(JSON_KEY_FOR_KEY, keyBase64);
		} catch (JSONException e) {
			throw new InvalidRequestException(e);
		}
		
		return json;
	}
}
