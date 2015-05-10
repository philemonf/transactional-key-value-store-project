package ch.epfl.tkvs.yarn.appmaster;

import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_MESSAGE_TYPE;
import static ch.epfl.tkvs.transactionmanager.communication.requests.TransactionManagerRequest.MESSAGE_TYPE;
import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;
import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;
import static ch.epfl.tkvs.yarn.appmaster.AppMaster.nextTransactionId;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.requests.TransactionManagerRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.TransactionManagerResponse;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;
import ch.epfl.tkvs.yarn.RemoteTransactionManager;
import ch.epfl.tkvs.yarn.RoutingTable;
import ch.epfl.tkvs.yarn.appmaster.centralized_decision.DeadlockCentralizedDecider;
import ch.epfl.tkvs.yarn.appmaster.centralized_decision.ICentralizedDecider;
import ch.epfl.tkvs.yarn.appmaster.centralized_decision.MinAliveTransactionDecider;


/**
 * The AM's processing thread, launched for processing received JSON messages.
 * @see ch.epfl.tkvs.yarn.appmaster.AppMaster
 * @see ch.epfl.tkvs.yarn.RoutingTable
 */
public class AMWorker extends Thread {

    private final static Logger log = Logger.getLogger(AMWorker.class.getName());

    private RoutingTable routing;
    private JSONObject jsonRequest;
    private Socket sock;
    private List<ICentralizedDecider> centralizedDeciders;

    public AMWorker(RoutingTable routing, JSONObject input, Socket sock) {
        this.routing = routing;
        this.jsonRequest = input;
        this.sock = sock;
        this.centralizedDeciders = asList(new DeadlockCentralizedDecider(), new MinAliveTransactionDecider());
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
            	for (ICentralizedDecider centralizedDecider : centralizedDeciders) {
	                if (centralizedDecider != null && centralizedDecider.shouldHandleMessageType(messageType)) {
	                    centralizedDecider.handleMessage(jsonRequest, sock);
	                    if (centralizedDecider.readyToDecide()) {
	                        centralizedDecider.performDecision();
	                    }
	                    
	                    // once a decider handled a request, we are done
	                    break;
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
        log.info("Assigned a TM to it: " + tm.getIp() + " - " + tm.getPort());
        int transactionID = nextTransactionId();
        return toJSON(new TransactionManagerResponse(true, transactionID, tm.getIp(), tm.getPort()));
    }
}
