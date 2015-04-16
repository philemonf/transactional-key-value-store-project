package ch.epfl.tkvs.yarn.appmaster;

import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;
import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.commons.math3.util.Pair;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.config.SlavesConfig;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.requests.TransactionManagerRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.TransactionManagerResponse;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;


public class AMWorker extends Thread {

    private String input;
    private Socket sock;
    private static Logger log = Logger.getLogger(AMWorker.class.getName());

    public AMWorker(String input, Socket sock) {
        this.input = input;
        this.sock = sock;
    }

    public void run() {
        try {
            // Create the response
            JSONObject jsonRequest = new JSONObject(input);
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
            }
            sock.close(); // Closing this socket will also close the socket's
                          // InputStream and OutputStream.
        } catch (IOException | JSONException | InvalidMessageException e) {
            log.error("Err", e);
        }
    }

    private JSONObject getResponseForRequest(TransactionManagerRequest request) throws JSONException, IOException {
        // TODO: Compute the hash of the key.
        int hash = 0;

        // Get the hostName and portNumber for that hash.
        SlavesConfig conf = new SlavesConfig();
        conf.getTMbyHash(hash);
        Pair<String, Integer> tm = conf.getTMbyHash(hash);

        // TODO: Create a unique transactionID
        int transactionID = 0;

        return toJSON(new TransactionManagerResponse(true, transactionID, tm.getKey(), tm.getValue()));
    }
}
