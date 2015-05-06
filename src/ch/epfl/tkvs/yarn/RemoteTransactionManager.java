package ch.epfl.tkvs.yarn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter;


/**
 * Represents a transaction manager that is remote compared to the node having this object.
 */
public class RemoteTransactionManager implements Serializable {

    private String hostname;
    private int port;

    public RemoteTransactionManager(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    /**
     * Send a message to the represented TM.
     * @param message the message to send
     * @param shouldWait whether one should wait for a response
     * @return the response or null if !shouldWait
     * @throws IOException in case of network failure or bad message format
     */
    public JSONObject sendMessage(Message message, boolean shouldWait) throws IOException {
        Socket sock = new Socket(hostname, port);
        PrintWriter out = new PrintWriter(sock.getOutputStream());

        try {
            out.println(Message2JSONConverter.toJSON(message).toString());
        } catch (JSONException e) {
            throw new IOException(e);
        }

        out.flush();

        JSONObject response = null;
        if (shouldWait) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            String input = reader.readLine();
            try {
                return new JSONObject(input);
            } catch (JSONException e) {
                throw new IOException(e);
            }
        }

        out.close();
        sock.close();

        return response;
    }
}