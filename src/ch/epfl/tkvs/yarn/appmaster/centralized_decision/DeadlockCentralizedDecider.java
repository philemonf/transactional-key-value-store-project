package ch.epfl.tkvs.yarn.appmaster.centralized_decision;

import static ch.epfl.tkvs.yarn.appmaster.AppMaster.log2;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Set;

import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.DeadlockInfoMessage;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;
import ch.epfl.tkvs.transactionmanager.lockingunit.DeadlockGraph;
import ch.epfl.tkvs.transactionmanager.lockingunit.DeadlockInfo;
import ch.epfl.tkvs.yarn.appmaster.AppMaster;


public class DeadlockCentralizedDecider implements ICentralizedDecider {

    private static HashMap<Integer, DeadlockGraph> graphs = new HashMap<Integer, DeadlockGraph>();
    private static HashMap<Integer, DeadlockGraph> secondGraphs = new HashMap<Integer, DeadlockGraph>();
    private static HashMap<Integer, Set<Integer>> activeTransactions = new HashMap<Integer, Set<Integer>>();

    @Override
    public synchronized void handleMessage(JSONObject message, Socket sock) {
        DeadlockInfoMessage dm = null;
        try {
            dm = (DeadlockInfoMessage) JSON2MessageConverter.parseJSON(message, DeadlockInfoMessage.class);
        } catch (InvalidMessageException e) {
            // TODO Handle the error
            log2.error(e, DeadlockCentralizedDecider.class);
            return;
        }

        // log.info(dm);
        DeadlockInfo info = null;
        try {
            info = dm.getGraph();
            // log.info(info);
        } catch (Exception e) {
            // TODO Handle the error
            log2.error("Cannot get deadlock info", e, DeadlockCentralizedDecider.class);
            return;

        }
        log2.info("Received messsage from " + info.getLocalHash(), DeadlockCentralizedDecider.class);
        if (graphs.containsKey(info.getLocalHash())) {
            secondGraphs.put(info.getLocalHash(), info.getGraph());
        } else {
            graphs.put(info.getLocalHash(), info.getGraph());
        }
        activeTransactions.put(info.getLocalHash(), info.getActiveTransactions());
    }

    @Override
    public synchronized boolean readyToDecide() {
        int totalTMCount = AppMaster.numberOfRegisteredTMs();
        log2.info("Ready to decide " + graphs.size() + "   " + totalTMCount + "   " + secondGraphs.isEmpty(), DeadlockCentralizedDecider.class);
        if (graphs.size() == totalTMCount)
            return true;
        if (!secondGraphs.isEmpty())
            return true;
        return false;
    }

    @Override
    public synchronized void performDecision() {
        DeadlockGraph mergedGraph = new DeadlockGraph(graphs.values());
        log2.info("perform decision", DeadlockCentralizedDecider.class);
        Set<Integer> transactionsToBeKilled = mergedGraph.checkForCycles();
        for (Integer tid : transactionsToBeKilled)
            log2.info("Killing transaction" + tid, DeadlockCentralizedDecider.class);
        sendKillMessages(transactionsToBeKilled);
        graphs = secondGraphs;
        secondGraphs = new HashMap<>();
    }

    private void sendKillMessages(Set<Integer> transactionsToBeKilled) {
        for (Integer transaction : transactionsToBeKilled) {
            AbortRequest abortRequest = new AbortRequest(transaction);
            for (Integer tm : activeTransactions.keySet()) {
                if (activeTransactions.get(tm).contains(transaction)) {
                    try {
                        AppMaster.sendMessageToTM(tm, abortRequest, true);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        log2.error("Cant send Abort ", e, DeadlockCentralizedDecider.class);
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
