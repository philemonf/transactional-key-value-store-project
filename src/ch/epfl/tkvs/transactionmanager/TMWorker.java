package ch.epfl.tkvs.transactionmanager;

import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;
import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;
import ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter;


public class TMWorker extends Thread {

    private Socket sock;
    private byte[] valueRead;
    private KeyValueStore kvStore;
    private static Logger log = Logger.getLogger(TMWorker.class.getName());

    public TMWorker(Socket sock, KeyValueStore kvStore) {
        this.sock = sock;
        this.kvStore = kvStore;
    }

    public void run() {
        try {

        	log.info("Transaction manager's worker thread just started...");
            // Read the request into a JSONObject
            BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            String inputStr = in.readLine();

            // Create the response
            JSONObject jsonRequest = new JSONObject(inputStr);
            JSONObject response = null;
            
            String requestType = jsonRequest.getString(JSONCommunication.KEY_FOR_MESSAGE_TYPE);
            
            switch (requestType) {
            
            case ReadRequest.MESSAGE_TYPE:
            	ReadRequest readRequest = (ReadRequest) JSON2MessageConverter.parseJSON(jsonRequest, ReadRequest.class);
                response = getResponseForRequest(readRequest);
                break;
                
            case WriteRequest.MESSAGE_TYPE:
            	WriteRequest writeRequest = (WriteRequest) JSON2MessageConverter.parseJSON(jsonRequest, WriteRequest.class);
                response = getResponseForRequest(writeRequest);
                break;
            }
            
            // Send the response
            log.info("Response" + response.toString());
            PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
            out.println(response.toString());

            in.close();
            out.close();
            sock.close();
        } catch (IOException | InvalidMessageException | JSONException e) {
            e.printStackTrace();
            
        }
    }
    
    private JSONObject getResponseForRequest(ReadRequest request) throws JSONException, IOException {
    	String encodedKey = request.getEncodedKey();
    	String encodedValue = kvStore.get(encodedKey);
    	
    	return toJSON(new ReadResponse(true, encodedValue));
    }
    
    private JSONObject getResponseForRequest(WriteRequest request) throws JSONException {
    	kvStore.put(request.getEncodedKey(), request.getEncodedValue());
    	
    	return toJSON(new GenericSuccessResponse());
    }

}
