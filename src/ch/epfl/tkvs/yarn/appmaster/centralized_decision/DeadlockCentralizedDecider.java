package ch.epfl.tkvs.yarn.appmaster.centralized_decision;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.DeadlockInfoMessage;
import ch.epfl.tkvs.transactionmanager.communication.DeadlockInfoMessage.DeadlockInfo;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;
import ch.epfl.tkvs.transactionmanager.lockingunit.DeadlockGraph;
import ch.epfl.tkvs.yarn.RoutingTable;
import ch.epfl.tkvs.yarn.appmaster.AppMaster;

public class DeadlockCentralizedDecider implements ICentralizedDecider {

	private static Logger log = Logger
			.getLogger(DeadlockCentralizedDecider.class.getName());
	private static HashMap<Integer, DeadlockGraph> graphs = new HashMap<Integer, DeadlockGraph>();
	private static HashMap<Integer, DeadlockGraph> secondGraphs = new HashMap<Integer, DeadlockGraph>();
	private static HashMap<Integer, Set<Integer>> activeTransactions = new HashMap<Integer, Set<Integer>>();
	private RoutingTable routing;

	public DeadlockCentralizedDecider(RoutingTable routingTable) {
		this.routing = routingTable;
	}

	@Override
	public synchronized void handleMessage(JSONObject message) {
		DeadlockInfoMessage dm = null;
		try {
			dm = (DeadlockInfoMessage) JSON2MessageConverter.parseJSON(message,
					DeadlockInfoMessage.class);
		} catch (InvalidMessageException e) {
			// TODO Handle the error
			log.error(e);
			return;
		}

		log.info(dm);
		DeadlockInfo info = null;
		try {
			info = dm.getGraph();
			log.info(info);
		} catch (ClassNotFoundException e) {
			// TODO Handle the error
			log.error(e);
			return;
		} catch (IOException e) {
			// TODO Handle the error
			log.error(e);
			return;
		}

		if (graphs.containsKey(info.getLocalHash())) {
			secondGraphs.put(info.getLocalHash(), info.getGraph());
		} else {
			graphs.put(info.getLocalHash(), info.getGraph());
		}
		activeTransactions.put(info.getLocalHash(), info.getActiveTransactions());
		return;
	}

	@Override
	public synchronized boolean readyToDecide() {
		int totalTMCount = AppMaster.numberOfRegisteredTMs();
		if (graphs.size()==totalTMCount)
			return true;
		if (!secondGraphs.isEmpty())
			return true;
		return false;
	}

	@Override
	public synchronized void performDecision() {
		DeadlockGraph mergedGraph = new DeadlockGraph(graphs.values());
		Set<Integer> transactionsToBeKilled = mergedGraph.checkForCycles();
		sendKillMessages(transactionsToBeKilled);
		graphs = secondGraphs;
		secondGraphs = new HashMap<Integer, DeadlockGraph>();
	}
	
	private void sendKillMessages(Set<Integer> transactionsToBeKilled) {
		for (Integer transaction : transactionsToBeKilled) {
			AbortRequest abortRequest = new AbortRequest(transaction);
			for (Integer tm : activeTransactions.keySet()) {
				if (activeTransactions.get(tm).contains(transaction)) {
					try {
						routing.findTM(tm).sendMessage(abortRequest, true);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	@Override
	public boolean shouldHandleMessageType(String messageType) {
		return messageType.equals(DeadlockInfoMessage.MESSAGE_TYPE);
	}

}
