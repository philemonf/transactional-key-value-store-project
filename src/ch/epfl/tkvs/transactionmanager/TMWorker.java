package ch.epfl.tkvs.transactionmanager;

import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.algorithms.CCAlgorithm;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.PrepareRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.TryCommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;


public class TMWorker extends Thread {

    private JSONObject jsonRequest;
    private Socket sock;
    private CCAlgorithm concurrencyController;
    private static Logger log = Logger.getLogger(TMWorker.class.getName());

    public TMWorker(JSONObject input, Socket sock, CCAlgorithm algorithm) {
        this.jsonRequest = input;
        this.sock = sock;

        this.concurrencyController = algorithm;
    }

    public void run() {
        try {
        	
            // Create the response
            Message response = null;
            Message request = null;
            String requestType = jsonRequest.getString(JSONCommunication.KEY_FOR_MESSAGE_TYPE);
            
            log.info("Just received: " + jsonRequest.toString());

            switch (requestType) {
            case BeginRequest.MESSAGE_TYPE:
                request = JSON2MessageConverter.parseJSON(jsonRequest, BeginRequest.class);
                BeginRequest beginRequest = (BeginRequest) request;
                log.info(beginRequest.toString());
                response = concurrencyController.begin(beginRequest);
                break;
            case ReadRequest.MESSAGE_TYPE:
                request = JSON2MessageConverter.parseJSON(jsonRequest, ReadRequest.class);
                ReadRequest readRequest = (ReadRequest) request;
                log.info(readRequest.toString());
                response = concurrencyController.read(readRequest);
                break;
            case WriteRequest.MESSAGE_TYPE:
                request = JSON2MessageConverter.parseJSON(jsonRequest, WriteRequest.class);
                WriteRequest writeRequest = (WriteRequest) request;
                log.info(writeRequest.toString());
                response = concurrencyController.write(writeRequest);
                break;
            case CommitRequest.MESSAGE_TYPE:
                request = JSON2MessageConverter.parseJSON(jsonRequest, CommitRequest.class);
                CommitRequest commitRequest = (CommitRequest) request;
                log.info(commitRequest.toString());
                response = concurrencyController.commit(commitRequest);
                break;
            case PrepareRequest.MESSAGE_TYPE:
                request = JSON2MessageConverter.parseJSON(jsonRequest, PrepareRequest.class);
                PrepareRequest prepareRequest = (PrepareRequest) request;
                log.info(prepareRequest.toString());
                response = concurrencyController.prepare(prepareRequest);
                break;
            case AbortRequest.MESSAGE_TYPE:
                request = JSON2MessageConverter.parseJSON(jsonRequest, AbortRequest.class);
                AbortRequest abortRequest = (AbortRequest) request;
                log.info(abortRequest.toString());
                response = concurrencyController.abort(abortRequest);
                break;
            case TryCommitRequest.MESSAGE_TYPE:
                request = JSON2MessageConverter.parseJSON(jsonRequest, TryCommitRequest.class);
                TryCommitRequest tr = (TryCommitRequest) request;
                log.info(tr.toString());
                response = concurrencyController.tryCommit(tr);
                break;
            }

            // Send the response
            if (response != null) {
                log.info(response + "<--" + request);
                PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
                out.println(toJSON(response).toString());
                out.close();
            } else {
            	log.info("NULL response to " + jsonRequest.toString());
            }

            sock.close(); // Closing this socket will also close the socket's
            // InputStream and OutputStream.
        } catch (IOException | InvalidMessageException | JSONException e) {
            e.printStackTrace();
        }
    }
}
