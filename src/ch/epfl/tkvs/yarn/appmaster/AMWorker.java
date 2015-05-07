package ch.epfl.tkvs.yarn.appmaster;

import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_MESSAGE_TYPE;
import static ch.epfl.tkvs.transactionmanager.communication.requests.TransactionManagerRequest.MESSAGE_TYPE;
import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;
import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;
import static ch.epfl.tkvs.yarn.appmaster.AppMaster.nextTransactionId;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.requests.TransactionManagerRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.TransactionManagerResponse;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;
import ch.epfl.tkvs.yarn.RemoteTransactionManager;
import ch.epfl.tkvs.yarn.RoutingTable;
import ch.epfl.tkvs.yarn.appmaster.centralized_decision.ICentralizedDecider;


/**
 * The AM's processing thread, launched for processing received JSON messages.
 * @see ch.epfl.tkvs.yarn.appmaster.AppMaster
 * @see ch.epfl.tkvs.yarn.RoutingTable
 */
public class AMWorker extends Thread {

    private RoutingTable routing;
    private JSONObject jsonRequest;
    private Socket sock;
    private ICentralizedDecider centralizedDecider;

    private static Logger log = Logger.getLogger(AMWorker.class.getName());

    public AMWorker(RoutingTable routing, JSONObject input, Socket sock, ICentralizedDecider decider) {
        this.routing = routing;
        this.jsonRequest = input;
        this.sock = sock;
        this.centralizedDecider = decider;
    }

    public void run() {
        try {
            // Create the response
            JSONObject response = null;

            String messageType = jsonRequest.getString(KEY_FOR_MESSAGE_TYPE);

            switch (messageType) {

            case MESSAGE_TYPE:
                TransactionManagerRequest request = (TransactionManagerRequest) parseJSON(jsonRequest, TransactionManagerRequest.class);
                response = getResponseForRequest(request);
                break;
            default:
                if (centralizedDecider != null && centralizedDecider.shouldHandleMessageType(messageType)) {
                    centralizedDecider.handleMessage(jsonRequest);
                    if (centralizedDecider.readyToDecide()) {
                        centralizedDecider.performDecision();
                    }
                }
            }

            // Send the response if it exists
            if (response != null) {
                log.info("Response" + response.toString());
                PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
                out.println(response.toString());
                out.close();
                log.info("Finish sending response " + response.toString());
            }
            sock.close(); // Closing this socket will also close the socket's InputStream and OutputStream.
        } catch (IOException | JSONException | InvalidMessageException e) {
            log.error(e);
        }
    }

    private JSONObject getResponseForRequest(TransactionManagerRequest request) throws JSONException, IOException {
        int localityHash = request.getLocalityHash();
        log.info("Get a transaction manager request for locality hash: " + localityHash);
        RemoteTransactionManager tm = routing.findTM(localityHash);
        log.info("Assigned a TM to it: " + tm.getHostname() + " - " + tm.getPort());
        int transactionID = nextTransactionId();
        return toJSON(new TransactionManagerResponse(true, transactionID, tm.getHostname(), tm.getPort()));
    }
}
