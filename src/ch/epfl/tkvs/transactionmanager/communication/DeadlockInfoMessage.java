package ch.epfl.tkvs.transactionmanager.communication;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
import ch.epfl.tkvs.transactionmanager.lockingunit.DeadlockGraph;

public class DeadlockInfoMessage extends Message {

	@JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
	public static final String MESSAGE_TYPE = "deadlock_message";

	@JSONAnnotation(key = JSONCommunication.KEY_FOR_DEAD_LOCK_GRAPH)
	private String encodedGraph;

	@JSONConstructor
	public DeadlockInfoMessage(int localHash, DeadlockGraph graph, Set<Integer> activeTransactions) throws IOException {
		encodedGraph = Base64Utils.convertToBase64(new DeadlockInfo(localHash, graph, activeTransactions));
	}

	/**
	 * Returns the dead lock graph contained in the message.
	 * 
	 * @return the dead lock graph if available or null (be careful)
	 * @throws ClassNotFoundException
	 *             in case the DeadlockGraph class is not in your classpath
	 * @throws IOException
	 *             in case of decoding problem
	 */
	public DeadlockInfo getGraph() throws ClassNotFoundException, IOException {
		if (encodedGraph == null) {
			return null;
		}

		return (DeadlockInfo) Base64Utils.convertFromBase64(encodedGraph);
	}

	public class DeadlockInfo implements Serializable {
		private static final long serialVersionUID = 1L;
		private int localHash;
		private DeadlockGraph graph;
		private Set<Integer> activeTransactions;

		public DeadlockInfo(int localHash, DeadlockGraph graph, Set<Integer> activeTransactions) {
			this.localHash = localHash;
			this.graph = graph;
			this.activeTransactions = activeTransactions;
		}

		public int getLocalHash() {
			return localHash;
		}

		public DeadlockGraph getGraph() {
			return graph;
		}
		
		public Set<Integer> getActiveTransactions() {
			return activeTransactions;
		}
	}
}
