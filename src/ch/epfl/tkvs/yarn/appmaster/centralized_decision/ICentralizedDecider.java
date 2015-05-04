package ch.epfl.tkvs.yarn.appmaster.centralized_decision;

import org.codehaus.jettison.json.JSONObject;

/**
 * Characterize the interface of centralized module used by the AppMaster to take global decision. 
 */
public interface ICentralizedDecider {
	
	/**
	 * Tells whether it should handle a message according to its type.
	 * @param messageType the string of the message type field
	 * @return true if and only if it can handle such message
	 */
	boolean shouldHandleMessageType(String messageType);
	
	/**
	 * Handle an incoming message.
	 * @param message a message to be handled
	 */
	void handleMessage(JSONObject message);
	
	/**
	 * @return true if the module is ready to make a decision
	 */
	boolean readyToDecide();

	/**
	 * Called whenever the module is ready to decide.
	 */
	void performDecision();
}
