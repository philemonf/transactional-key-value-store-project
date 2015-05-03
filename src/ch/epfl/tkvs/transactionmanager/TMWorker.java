package ch.epfl.tkvs.transactionmanager;

import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.algorithms.Algorithm;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;


public class TMWorker extends Thread {

    private JSONObject jsonRequest;
    private Socket sock;
    private Algorithm concurrencyController;
    private static Logger log = Logger.getLogger(TMWorker.class.getName());

    public TMWorker(JSONObject input, Socket sock, Algorithm algorithm) {
        this.jsonRequest = input;
        this.sock = sock;

        this.concurrencyController = algorithm;
    }

    public void run() {
        try {

            // Create the response
            JSONObject response = null;

            String requestType = jsonRequest.getString(JSONCommunication.KEY_FOR_MESSAGE_TYPE);

            switch (requestType) {
            case BeginRequest.MESSAGE_TYPE:
                BeginRequest beginRequest = (BeginRequest) JSON2MessageConverter.parseJSON(jsonRequest, BeginRequest.class);
                response = toJSON(concurrencyController.begin(beginRequest));
                break;
            case ReadRequest.MESSAGE_TYPE:
                ReadRequest readRequest = (ReadRequest) JSON2MessageConverter.parseJSON(jsonRequest, ReadRequest.class);
                response = toJSON(concurrencyController.read(readRequest));
                break;
            case WriteRequest.MESSAGE_TYPE:
                WriteRequest writeRequest = (WriteRequest) JSON2MessageConverter.parseJSON(jsonRequest, WriteRequest.class);
                response = toJSON(concurrencyController.write(writeRequest));
                break;
            case CommitRequest.MESSAGE_TYPE:
                CommitRequest commitRequest = (CommitRequest) JSON2MessageConverter.parseJSON(jsonRequest, CommitRequest.class);
                response = toJSON(concurrencyController.commit(commitRequest));
                break;
            }

            // Send the response
            if (response != null) {
                log.info("Response" + response.toString());
                PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
                out.println(response.toString());
            }
            sock.close(); // Closing this socket will also close the socket's
            // InputStream and OutputStream.
        } catch (IOException | InvalidMessageException | JSONException e) {
            e.printStackTrace();
        }
    }

}
