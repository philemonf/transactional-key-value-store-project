package ch.epfl.tkvs.transactionmanager.communication;

import java.io.IOException;

import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
import ch.epfl.tkvs.yarn.RoutingTable;


public class TMInitMessage extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "tm_init_message";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_ALGO_CONFIG)
    private String algoConfig;
    
    @JSONAnnotation(key = JSONCommunication.KEY_FOR_ROUTING_TABLE)
    private String encodedRoutingTable;

    @JSONConstructor
    public TMInitMessage(RoutingTable rt, String algoConfig) throws IOException {
        encodedRoutingTable = Base64Utils.convertToBase64(rt);
        this.algoConfig = algoConfig;
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
    
    /**
     * Can return "mvto", "simple_2pl" or "mvcc2pl".
     * @return The concurrency control algorithm selected by the user.
     */
    public String getConcurrencyControlConfig() {
    	return algoConfig;
    }
}
