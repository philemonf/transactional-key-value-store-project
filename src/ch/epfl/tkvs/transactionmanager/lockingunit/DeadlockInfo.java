package ch.epfl.tkvs.transactionmanager.lockingunit;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


/**
 * This class is a wrapper for the information regarding the deadlock graphs to be sent to the centralized decider.
 */
public class DeadlockInfo implements Serializable {

    private static final long serialVersionUID = 1L;
    private int localHash;
    private DeadlockGraph graph;
    private Set<Integer> activeTransactions;

    /**
     * @param localHash hash describing the particular transaction manager (TM) which sends this info
     * @param graph the DeadlockGraph held by the LockingUnit of the given TM
     * @param activeTransactions the transactions which were alive on the given TM at the time of the creation of this
     * information.
     */
    public DeadlockInfo(int localHash, DeadlockGraph graph, HashSet<Integer> activeTransactions) {
        this.localHash = localHash;
        this.graph = graph;
        this.activeTransactions = activeTransactions;
    }

    /**
     * @return the locality hash which distinguish the TM sending this information.
     */
    public int getLocalHash() {
        return localHash;
    }

    /**
     * @return the DeadlockGraph held by the TM which sends this information
     */
    public DeadlockGraph getGraph() {
        return graph;
    }

    /**
     * @return the transactions which were alive on the given TM at the time of the creation of this information.
     */
    public Set<Integer> getActiveTransactions() {
        return activeTransactions;
    }
}
