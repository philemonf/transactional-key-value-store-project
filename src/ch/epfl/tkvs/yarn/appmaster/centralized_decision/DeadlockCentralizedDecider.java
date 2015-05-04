package ch.epfl.tkvs.yarn.appmaster.centralized_decision;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.DeadlockMessage;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter.InvalidMessageException;
import ch.epfl.tkvs.yarn.appmaster.AppMaster;

public class DeadlockCentralizedDecider implements ICentralizedDecider {

	private static Logger log = Logger.getLogger(DeadlockCentralizedDecider.class.getName());
	
	@Override
	public void handleMessage(JSONObject message) {
		
		DeadlockMessage dm = null;
		
		try {
			dm = (DeadlockMessage) JSON2MessageConverter.parseJSON(message, DeadlockMessage.class);
		} catch (InvalidMessageException e) {
			// TODO Handle the error
			log.error(e);
		}

		log.info(dm);

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
		return messageType.equals(DeadlockMessage.MESSAGE_TYPE);
	}
	
}
