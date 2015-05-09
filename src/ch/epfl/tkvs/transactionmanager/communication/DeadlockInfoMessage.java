package ch.epfl.tkvs.transactionmanager.communication;

import java.io.IOException;

import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
import ch.epfl.tkvs.transactionmanager.lockingunit.DeadlockGraph;
import ch.epfl.tkvs.transactionmanager.lockingunit.DeadlockInfo;
import java.util.HashSet;


public class DeadlockInfoMessage extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "deadlock_message";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_DEAD_LOCK_GRAPH)
    private String encodedGraph;

    public DeadlockInfoMessage(DeadlockInfo di) throws IOException {
        encodedGraph = Base64Utils.convertToBase64(di);
    }

    @JSONConstructor
    public DeadlockInfoMessage(String encodedGraph) {
        this.encodedGraph = encodedGraph;
    }

    /**
     * Returns the dead lock graph contained in the message.
     * 
     * @return the dead lock graph if available or null (be careful)
     * @throws ClassNotFoundException in case the DeadlockGraph class is not in your classpath
     * @throws IOException in case of decoding problem
     */
    public DeadlockInfo getGraph() throws ClassNotFoundException, IOException {
        if (encodedGraph == null) {
            return null;
        }

        return (DeadlockInfo) Base64Utils.convertFromBase64(encodedGraph);
    }

}
