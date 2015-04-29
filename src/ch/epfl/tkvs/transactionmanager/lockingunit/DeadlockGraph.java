package ch.epfl.tkvs.transactionmanager.lockingunit;

import java.util.HashMap;
import java.util.HashSet;

public class DeadlockGraph {

	HashMap<Integer, HashSet<Integer>> outgoingEdges;
	HashMap<Integer, HashSet<Integer>> incomingEdges;

	public DeadlockGraph() {
		outgoingEdges = new HashMap<Integer, HashSet<Integer>>();
		incomingEdges = new HashMap<Integer, HashSet<Integer>>();
	}

	public boolean isCyclicAfter(int transactionID,
			HashSet<Integer> incompatibleTransactions) {
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
			cyclic = checkForCycle(transactionID, visited, onStack);
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

	private boolean checkForCycle(Integer node, HashSet<Integer> visited, HashSet<Integer> onStack) {
		visited.add(node);
		if (outgoingEdges.get(node) != null) {
			onStack.add(node);
			for (Integer neighbor : outgoingEdges.get(node)) {
				if (!visited.contains(neighbor)) {
					if (checkForCycle(neighbor, visited, onStack))
						return true;
				} else if (onStack.contains(neighbor)) {
					return true;
				}
			}
			onStack.remove(node);
		}
		return false;
	}

	public void addDependencies(int transactionID,
			HashSet<Integer> incompatibleTransactions) {
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

}
