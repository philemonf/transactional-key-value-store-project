package ch.epfl.tkvs.transactionmanager.lockingunit;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


public class DeadlockInfo implements Serializable {

    private static final long serialVersionUID = 1L;
    private int localHash;
    private DeadlockGraph graph;
    private Set<Integer> activeTransactions;

    public DeadlockInfo(int localHash, DeadlockGraph graph, HashSet<Integer> activeTransactions) {
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
