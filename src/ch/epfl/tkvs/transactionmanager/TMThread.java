package ch.epfl.tkvs.transactionmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;
import ch.epfl.tkvs.transactionmanager.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.requests.Request;
import ch.epfl.tkvs.transactionmanager.requests.Request.InvalidRequestException;
import ch.epfl.tkvs.transactionmanager.requests.Response;
import ch.epfl.tkvs.transactionmanager.requests.WriteRequest;


public class TMThread extends Thread {

    private Socket sock;
    private byte[] valueRead;
    private KeyValueStore kvStore;
    private static Logger log = Logger.getLogger(TMThread.class.getName());

    public TMThread(Socket sock, KeyValueStore kvStore) {
        this.sock = sock;
        this.kvStore = kvStore;
    }

    public void run() {
        try {

            // Read the request into a JSONObject
            BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            String inputStr = in.readLine();

            // Create the response
            JSONObject jsonRequest = new JSONObject(inputStr);
            JSONObject response = null;
            
            String requestType = jsonRequest.getString(Request.JSON_KEY_FOR_REQUEST_TYPE);
            
            switch (requestType) {
            
            case ReadRequest.TYPE:
                response = getResponseForRequest(new ReadRequest(jsonRequest));
                break;
            case WriteRequest.TYPE:
                response = getResponseForRequest(new WriteRequest(jsonRequest));
                break;
           default:
                response = getErrorResponse("Unsupported operation: " + requestType);
                break;
            }

            // Send the response
            log.info("Response" + response.toString());
            PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
            out.println(response.toString());

            in.close();
            out.close();
            sock.close();
        } catch (IOException | JSONException | InvalidRequestException e) {
            e.printStackTrace();
            
        }
    }
    
    private JSONObject getResponseForRequest(ReadRequest request) throws JSONException {
    	String encodedKey = request.getKeyBase64();
    	String encodedValue = kvStore.get(encodedKey);
    	return Response.toJSON(encodedKey, encodedValue);
    }
    
    private JSONObject getResponseForRequest(WriteRequest request) throws JSONException {
    	kvStore.put(request.getKeyBase64(), request.getValueKeyBase64());
    	
    	return getSuccessResponse("Key/value successfully written.");
    }
    
    private JSONObject getSuccessResponse(String message) throws JSONException {
    	JSONObject json = new JSONObject();
    	json.put(Response.JSON_KEY_FOR_RESPONSE_TYPE, Response.JSON_VALUE_FOR_SUCCESS);
    	json.put(Response.JSON_KEY_FOR_MESSAGE, message);
    	return json;
    }
    
    private JSONObject getErrorResponse(String message) throws JSONException {
    	JSONObject json = new JSONObject();
    	json.put(Response.JSON_KEY_FOR_RESPONSE_TYPE, Response.JSON_VALUE_FOR_ERROR);
    	json.put(Response.JSON_KEY_FOR_MESSAGE, message);
    	return json;
    }

}
