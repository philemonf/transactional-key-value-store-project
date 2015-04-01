package ch.epfl.tkvs.yarn.appmaster;

import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;
import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.config.SlavesConfig;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.requests.TransactionManagerRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.TransactionManagerResponse;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;


public class AMWorker extends Thread {

    private Socket sock;
    private static Logger log = Logger.getLogger(AMWorker.class.getName());

    public AMWorker(Socket sock) {
        this.sock = sock;
    }

    public void run() {
        try {
            // Read the request into a JSONObject
            BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            String inputStr = in.readLine();

            // Create the response
            JSONObject jsonRequest = new JSONObject(inputStr);
            JSONObject response = null;
            
            switch (jsonRequest.getString(JSONCommunication.KEY_FOR_MESSAGE_TYPE)) {
            
            case TransactionManagerRequest.MESSAGE_TYPE:
            	TransactionManagerRequest request = (TransactionManagerRequest) parseJSON(jsonRequest, TransactionManagerRequest.class);
                response = getResponseForRequest(request);
                break;
            }

            // Send the response
            log.info("Response" + response.toString());
            
            if (response != null) {
            	PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
            	out.println(response.toString());
                out.close();
            }

            in.close();
            sock.close();
        } catch (IOException | JSONException | InvalidMessageException e) {
            log.error("Err", e);
        }
    }

    private JSONObject getResponseForRequest(TransactionManagerRequest request) throws JSONException, IOException {
        // TODO: Compute the hash of the key.
        int hash = 0;
        
        // Get the hostName and portNumber for that hash.
        SlavesConfig conf = new SlavesConfig();
    	String hostName = conf.getHosts()[hash];
        int portNumber = conf.getPortForTransactionManager(hash);
        
        // TODO: Create a unique transactionID
        int transactionID = 0;
        
        return toJSON(new TransactionManagerResponse(true, transactionID, hostName,  portNumber));
    }
}
