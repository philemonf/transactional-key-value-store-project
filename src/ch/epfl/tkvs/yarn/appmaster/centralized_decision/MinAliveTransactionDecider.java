package ch.epfl.tkvs.yarn.appmaster.centralized_decision;

import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;
import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.TransactionTerminateMessage;
import ch.epfl.tkvs.transactionmanager.communication.responses.MinAliveTransactionResponse;


/**
 * Used by the garbage collector of MVTO to get the alive transaction with the minimum timestamp from the AppMaster.
 */
public class MinAliveTransactionDecider implements ICentralizedDecider {

    private int minAlive = 0;
    private Set<Integer> terminated = new HashSet<Integer>();
    private Queue<Socket> waitQueue = new ConcurrentLinkedQueue<Socket>();
    private final static Logger log = Logger.getLogger(MinAliveTransactionDecider.class.getName());

    @Override
    public boolean shouldHandleMessageType(String messageType) {
        return TransactionTerminateMessage.MESSAGE_TYPE.equals(messageType);
    }

    @Override
    public void handleMessage(JSONObject message, Socket sock) {
        // log.info("handle " + message.toString() + " from " + sock.getInetAddress());
        try {

            TransactionTerminateMessage tMessage = (TransactionTerminateMessage) parseJSON(message, TransactionTerminateMessage.class);
            List<Integer> tids = tMessage.getTransactionIds();
            updateWithTerminated(tids);
            waitQueue.add(sock);

        } catch (Exception e) {
            log.error(e);
        }
    }

    @Override
    public boolean readyToDecide() {
        return !waitQueue.isEmpty();
    }

    @Override
    public void performDecision() {
        // log.info("performDecision: " + minAlive);
        try {
            MinAliveTransactionResponse minAliveRes = new MinAliveTransactionResponse(minAlive);
            JSONObject json = toJSON(minAliveRes);

            Socket sock = null;
            while ((sock = waitQueue.poll()) != null) {
                PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
                out.println(json.toString());
                out.close(); // do not close sock, will be closed outside
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    private synchronized void updateWithTerminated(List<Integer> tids) {
        terminated.addAll(tids);

        Integer iMinAlive = new Integer(minAlive);
        while (terminated.contains(iMinAlive)) {
            terminated.remove(iMinAlive);

            minAlive++;
            iMinAlive = new Integer(minAlive);
        }

        // log.info(tids + " - " + minAlive);
    }

}
