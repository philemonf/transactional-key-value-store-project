package ch.epfl.tkvs.transactionmanager.communication;

import java.io.IOException;

import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
import ch.epfl.tkvs.transactionmanager.lockingunit.DeadlockGraph;

public class DeadlockMessage extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "deadlock_message";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_DEAD_LOCK_GRAPH)
    private String encodedGraph;
    
    public DeadlockMessage(DeadlockGraph graph) throws IOException {
    	encodedGraph = Base64Utils.convertToBase64(graph);
    }
    
    /**
     * Returns the dead lock graph contained in the message.
     * @return the dead lock graph if available or null (be careful)
     * @throws ClassNotFoundException in case the DeadlockGraph class is not in your classpath
     * @throws IOException in case of decoding problem
     */
    public DeadlockGraph getGraph() throws ClassNotFoundException, IOException {
    	if (encodedGraph == null) {
    		return null;
    	}
    	
    	return (DeadlockGraph) Base64Utils.convertFromBase64(encodedGraph);
    }
}
