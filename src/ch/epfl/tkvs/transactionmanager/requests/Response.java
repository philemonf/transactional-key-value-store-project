package ch.epfl.tkvs.transactionmanager.requests;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Response {
	
	public static final String JSON_KEY_FOR_RESPONSE_TYPE = "response_type";
	public static final String JSON_KEY_FOR_MESSAGE = "message";
	
	public static final String JSON_VALUE_FOR_SUCCESS = "success";
	public static final String JSON_VALUE_FOR_ERROR = "error";
	
	
	public static JSONObject toJSON(String encodedKey, String encodedValue) throws JSONException {
		JSONObject json = new JSONObject();
		
		json.put(Request.JSON_KEY_FOR_KEY, encodedKey);
		json.put(Request.JSON_KEY_FOR_VALUE, encodedValue);
		
		return json;
	}
}
