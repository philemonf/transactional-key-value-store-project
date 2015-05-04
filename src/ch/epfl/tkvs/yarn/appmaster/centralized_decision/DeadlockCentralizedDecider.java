package ch.epfl.tkvs.yarn.appmaster.centralized_decision;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.DeadlockInfoMessage;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;
import ch.epfl.tkvs.transactionmanager.lockingunit.DeadlockGraph;
import ch.epfl.tkvs.yarn.appmaster.AppMaster;

public class DeadlockCentralizedDecider implements ICentralizedDecider {

	private static Logger log = Logger.getLogger(DeadlockCentralizedDecider.class.getName());
	
	@Override
	public void handleMessage(JSONObject message) {
		
		DeadlockInfoMessage dm = null;
		
		try {
			dm = (DeadlockInfoMessage) JSON2MessageConverter.parseJSON(message, DeadlockInfoMessage.class);
		} catch (InvalidMessageException e) {
			// TODO Handle the error
			log.error(e);
			return;
		}
		
		log.info(dm);
		
		DeadlockGraph graph = null;
		
		try {
			
			graph = dm.getGraph();		
			log.info(graph);
			
		} catch (ClassNotFoundException e) {
			// TODO Handle the error
			log.error(e);
			return;
		} catch (IOException e) {
			// TODO Handle the error
			log.error(e);
			return;
		}
		

		// TODO: Do something
	}

	@Override
	public boolean readyToDecide() {
		return false;
	}

	@Override
	public void performDecision() {
		// TODO Perform what is needed
	}

	@Override
	public boolean shouldHandleMessageType(String messageType) {
		return messageType.equals(DeadlockInfoMessage.MESSAGE_TYPE);
	}
	
}
