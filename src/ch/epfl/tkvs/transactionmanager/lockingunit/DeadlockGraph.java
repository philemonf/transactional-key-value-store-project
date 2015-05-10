package ch.epfl.tkvs.transactionmanager.lockingunit;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


public class DeadlockGraph implements Serializable {

    private static final long serialVersionUID = 1L;
    HashMap<Integer, HashSet<Integer>> outgoingEdges;
    HashMap<Integer, HashSet<Integer>> incomingEdges;

    public DeadlockGraph() {
        outgoingEdges = new HashMap<Integer, HashSet<Integer>>();
        incomingEdges = new HashMap<Integer, HashSet<Integer>>();
    }

    /**
     * 
     * @param graphs a collection of DeadlockGraph s to be merged to form this DeadlockGraph
     */
    public DeadlockGraph(Collection<DeadlockGraph> graphs) {
        outgoingEdges = new HashMap<Integer, HashSet<Integer>>();
        incomingEdges = new HashMap<Integer, HashSet<Integer>>();
        for (DeadlockGraph graph : graphs) {
            for (Integer key : graph.outgoingEdges.keySet()) {
                if (!outgoingEdges.containsKey(key)) {
                    outgoingEdges.put(key, new HashSet<Integer>());
                }
                outgoingEdges.get(key).addAll(graph.outgoingEdges.get(key));
            }
            for (Integer key : graph.incomingEdges.keySet()) {
                if (!incomingEdges.containsKey(key)) {
                    incomingEdges.put(key, new HashSet<Integer>());
                }
                incomingEdges.get(key).addAll(graph.incomingEdges.get(key));
            }
        }
    }

    /**
     * Given edges are added to the graph iff they do not cause a cycle.
     * 
     * @param transactionID ID of the transaction which is waiting for other transactions
     * @param incompatibleTransactions IDs of the transactions that the given transaction started to wait for
     * @return true if the given dependencies cause a deadlock, false otherwise.
     */
    public boolean isCyclicAfter(int transactionID, HashSet<Integer> incompatibleTransactions) {
        boolean cyclic;
        if (!outgoingEdges.containsKey(transactionID)) {
            outgoingEdges.put(transactionID, new HashSet<Integer>());
        }
        HashSet<Integer> newDependencies = new HashSet<Integer>();
        for (Integer incompatible : incompatibleTransactions) {
            if (!outgoingEdges.get(transactionID).contains(incompatible)) {
                outgoingEdges.get(transactionID).add(incompatible);
                newDependencies.add(incompatible);
            }
        }

        if (newDependencies.isEmpty())
            cyclic = false;
        else {
            HashSet<Integer> visited = new HashSet<Integer>();
            HashSet<Integer> onStack = new HashSet<Integer>();
            cyclic = checkForCycle(transactionID, visited, onStack) == null ? false : true;
        }

        if (cyclic) {
            outgoingEdges.get(transactionID).removeAll(newDependencies);
            return true;
        } else {
            for (Integer incompatible : incompatibleTransactions) {
                if (!incomingEdges.containsKey(incompatible)) {
                    incomingEdges.put(incompatible, new HashSet<Integer>());
                }
                if (!incomingEdges.get(incompatible).contains(transactionID)) {
                    incomingEdges.get(incompatible).add(transactionID);
                }
            }
            return false;
        }
    }

    /**
     * 
     * @param node start node of the DFS
     * @param visited set of nodes which are already visited
     * @param onStack set of nodes leading from the root to the current node. this parameter expected to be non-empty
     * only when the method is called by itself.
     * @return ID of a transaction which is a part of the deadlock.
     */
    private Integer checkForCycle(Integer node, HashSet<Integer> visited, HashSet<Integer> onStack) {
        Integer branchResult;
        visited.add(node);
        if (outgoingEdges.get(node) != null) {
            onStack.add(node);
            for (Integer neighbor : outgoingEdges.get(node)) {
                if (!visited.contains(neighbor)) {
                    branchResult = checkForCycle(neighbor, visited, onStack);
                    if (branchResult != null)
                        return branchResult;
                } else if (onStack.contains(neighbor)) {
                    return neighbor;
                }
            }
            onStack.remove(node);
        }
        return null;
    }

    /**
     * 
     * @param transactionID ID of the transaction which is waiting for other transactions.
     * @param incompatibleTransactions IDs of the transactions that the given transaction started to wait for.
     */
    public void addDependencies(int transactionID, HashSet<Integer> incompatibleTransactions) {
        if (!outgoingEdges.containsKey(transactionID)) {
            outgoingEdges.put(transactionID, new HashSet<Integer>());
        }
        for (Integer incompatible : incompatibleTransactions) {
            if (!outgoingEdges.get(transactionID).contains(incompatible)) {
                outgoingEdges.get(transactionID).add(incompatible);
            }
        }
        for (Integer incompatible : incompatibleTransactions) {
            if (!incomingEdges.containsKey(incompatible)) {
                incomingEdges.put(incompatible, new HashSet<Integer>());
            }
            if (!incomingEdges.get(incompatible).contains(transactionID)) {
                incomingEdges.get(incompatible).add(transactionID);
            }
        }
    }

    /**
     * Removes a node with all of its edges
     * 
     * @param transactionID ID of the transaction whose node is to be removed
     */
    public void removeTransaction(int transactionID) {
        if (outgoingEdges.get(transactionID) != null) {
            for (Integer next : outgoingEdges.get(transactionID)) {
                incomingEdges.get(next).remove(transactionID);
            }
            outgoingEdges.remove(transactionID);
        }
        if (incomingEdges.get(transactionID) != null) {
            for (Integer previous : incomingEdges.get(transactionID)) {
                outgoingEdges.get(previous).remove(transactionID);
            }
            incomingEdges.remove(transactionID);
        }
    }

    /**
     * Creates a new DeadlockGraph by cloning its outgoingEdges only. Used to create the graph to be sent to the
     * centralized deadlock detection unit.
     * 
     * @return a new -independent- DeadlockGraph
     */
    public DeadlockGraph copyOutgoingEdges() {
        DeadlockGraph copy = new DeadlockGraph();
        for (Integer key : outgoingEdges.keySet()) {
            if (outgoingEdges.get(key) == null || outgoingEdges.get(key).isEmpty()) {
                continue;
            }
            copy.outgoingEdges.put(key, new HashSet<Integer>());
            copy.outgoingEdges.get(key).addAll(outgoingEdges.get(key));
        }
        return copy;
    }

    /**
     * Detects and removes some transactions to make the graph acyclic
     * 
     * @return set of IDs of the transactions to be removed to make this graph acyclic.
     */
    public Set<Integer> checkForCycles() {
        HashSet<Integer> transactionsToBeKilled = new HashSet<Integer>();
        HashSet<Integer> visited = new HashSet<Integer>();
        HashSet<Integer> previouslyVisited = new HashSet<Integer>();
        HashSet<Integer> onStack;

        Integer transactionInCycle;
        HashSet<Integer> originalKeySet = new HashSet<Integer>(outgoingEdges.keySet());
        for (Integer node : originalKeySet) {
            if (visited.contains(node))
                continue;
            while (true) {
                onStack = new HashSet<Integer>();
                transactionInCycle = checkForCycle(node, visited, onStack);
                if (transactionInCycle == null) { // if not cyclic
                    previouslyVisited = new HashSet<Integer>(visited);
                    break;
                } else { // if cyclic
                    transactionsToBeKilled.add(transactionInCycle);
                    previouslyVisited.add(transactionInCycle);
                    visited = new HashSet<Integer>(previouslyVisited);
                    removeTransaction(transactionInCycle);
                }
            }
            if (visited.size() == originalKeySet.size())
                break;
        }
        return transactionsToBeKilled;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (Integer key : outgoingEdges.keySet()) {
            result.append(key);
            result.append("\n");
            for (Integer neighbor : outgoingEdges.get(key)) {
                result.append("\t");
                result.append(neighbor);
                result.append("\n");
            }
        }
        return result.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other == this)
            return true;
        if (!(other instanceof DeadlockGraph))
            return false;
        DeadlockGraph otherGraph = (DeadlockGraph) other;
        return outgoingEdges.equals(otherGraph.outgoingEdges) && incomingEdges.equals(otherGraph.incomingEdges);
    }
}
