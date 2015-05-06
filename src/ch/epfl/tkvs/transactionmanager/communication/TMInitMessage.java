package ch.epfl.tkvs.transactionmanager.communication;

import java.io.IOException;

import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
import ch.epfl.tkvs.yarn.RoutingTable;

public class TMInitMessage extends Message {
	
	@JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "tm_init_message";
	
	@JSONAnnotation(key = JSONCommunication.KEY_FOR_ROUTING_TABLE)
	private String encodedRoutingTable;
	
	public TMInitMessage(RoutingTable rt) throws IOException {
		encodedRoutingTable = Base64Utils.convertToBase64(rt);
	}
	
	/**
	 * Return the routing table sent by the AppMaster to the TM for init.
	 * @return a filled routing table or null in case of failure
	 */
	public RoutingTable getRoutingTable() {
		try {
			return (RoutingTable) Base64Utils.convertFromBase64(encodedRoutingTable);
		} catch (ClassNotFoundException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
	}
}
