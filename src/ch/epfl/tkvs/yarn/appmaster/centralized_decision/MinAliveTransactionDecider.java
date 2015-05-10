package ch.epfl.tkvs.yarn.appmaster.centralized_decision;

import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_MESSAGE_TYPE;
import static ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.parseJSON;
import static ch.epfl.tkvs.transactionmanager.communication.utils.Message2JSONConverter.toJSON;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.TransactionTerminateMessage;
import ch.epfl.tkvs.transactionmanager.communication.requests.MinAliveTransactionRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.MinAliveTransactionResponse;

/**
 * Used by the garbage collector of MVTO to get the alive transaction with the minimum
 * timestamp from the AppMaster.
 */
public class MinAliveTransactionDecider implements ICentralizedDecider {

	private int minAlive = 0;
	private Set<Integer> terminated = new ConcurrentSkipListSet<Integer>();
	private Queue<Socket> waitQueue = new ConcurrentLinkedQueue<Socket>();
	private final static Logger log = Logger
			.getLogger(MinAliveTransactionDecider.class.getName());

	@Override
	public boolean shouldHandleMessageType(String messageType) {
		return TransactionTerminateMessage.MESSAGE_TYPE.equals(messageType)
				|| MinAliveTransactionRequest.MESSAGE_TYPE.equals(messageType);
	}

	@Override
	public void handleMessage(JSONObject message, Socket sock) {
		log.info("handle " + message.toString() + " from " + sock.getInetAddress());
		try {
			String messageType = message.getString(KEY_FOR_MESSAGE_TYPE);

			if (messageType.equals(TransactionTerminateMessage.MESSAGE_TYPE)) {
				
				TransactionTerminateMessage tMessage = (TransactionTerminateMessage) parseJSON(message, TransactionTerminateMessage.class);
				int tid = tMessage.getTransactionId();

				if (tid == minAlive) {
					++minAlive;
					updateWithTerminated();
				} else {
					terminated.add(tid);
				}
			} else {
				waitQueue.add(sock);
			}

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
		log.info("performDecision: " + minAlive);
		try {
			MinAliveTransactionResponse minAliveRes = new MinAliveTransactionResponse(minAlive);
			JSONObject json = toJSON(minAliveRes);
			
			Socket sock = null;
			while ((sock = waitQueue.poll()) != null) {
				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
				out.println(json.toString());
				out.close(); //do not close sock, will be closed outside
			}
		} catch (Exception e) {
			log.error(e);
		}
	}

	private synchronized void updateWithTerminated() {
		Integer iMinAlive = new Integer(minAlive);
		while (terminated.contains(iMinAlive)) {
			terminated.remove(iMinAlive);
			
			minAlive++;
			iMinAlive = new Integer(minAlive);
		}
	}

}
