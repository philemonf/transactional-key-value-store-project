package ch.epfl.tkvs.yarn.appmaster.centralized_decision;

import static ch.epfl.tkvs.yarn.appmaster.AppMaster.log2;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;
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
    private static HashMap<Integer, Set<Integer>> activeTransactions = new HashMap<Integer, Set<Integer>>();

    private static Logger log = Logger.getLogger(DeadlockCentralizedDecider.class);

    @Override
    public synchronized void handleMessage(JSONObject message, Socket sock) {
        log.info("DeadlockCentralizedDecider's handleMessage is called");
        DeadlockInfoMessage dm = null;
        try {
            dm = (DeadlockInfoMessage) JSON2MessageConverter.parseJSON(message, DeadlockInfoMessage.class);
        } catch (InvalidMessageException e) {
            // TODO Handle the error
            log.error(e);
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
        log.info("Received messsage from " + info.getLocalHash());
        for (Integer active : info.getActiveTransactions()) {
            log.info("Activetransaction: " + active);
        }
        log.info("\n" + info.getGraph());
        graphs.put(info.getLocalHash(), info.getGraph());
        activeTransactions.put(info.getLocalHash(), info.getActiveTransactions());
    }

    @Override
    public synchronized boolean readyToDecide() {
        int totalTMCount = AppMaster.numberOfRegisteredTMs();
        log.info("graph_size = " + graphs.size() + " - total_expected=" + totalTMCount);
        if (graphs.size() == totalTMCount)
            return true;
        return false;
    }

    @Override
    public synchronized void performDecision() {
        log.info("perform decision");
        DeadlockGraph mergedGraph = new DeadlockGraph(graphs.values());
        log.info("\n" + mergedGraph);
        Set<Integer> transactionsToBeKilled = mergedGraph.checkForCycles();

        for (Integer tid : transactionsToBeKilled)
            log.info("Killing transaction" + tid);
        sendKillMessages(transactionsToBeKilled);
        graphs.clear();
    }

    private void sendKillMessages(Set<Integer> transactionsToBeKilled) {
        for (Integer transaction : transactionsToBeKilled) {
            AbortRequest abortRequest = new AbortRequest(transaction);
            for (Integer tm : activeTransactions.keySet()) {
                if (activeTransactions.get(tm).contains(transaction)) {
                    try {
                        AppMaster.sendMessageToTM(tm, abortRequest, false);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        log.error("Cant send Abort " + e);
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
