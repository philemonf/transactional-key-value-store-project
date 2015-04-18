package ch.epfl.tkvs.transactionmanager.algorithms;

import java.util.HashMap;
import java.util.HashSet;

public class DeadlockGraph {
	HashMap<Integer, HashSet<Integer>> outgoingEdges;
	HashMap<Integer, HashSet<Integer>> incomingEdges;

	public DeadlockGraph() {
		outgoingEdges = new HashMap<Integer, HashSet<Integer>>();
		incomingEdges = new HashMap<Integer, HashSet<Integer>>();
	}

	public boolean isCyclicAfter(int transactionId,
			HashSet<Integer> incompatibleTransactions) {
		if (!outgoingEdges.containsKey(transactionId)) {
			outgoingEdges.put(transactionId, new HashSet<Integer>());
		}
		HashSet<Integer> newDependencies = new HashSet<Integer>();
		for (Integer incompatible : incompatibleTransactions) {
			if (!outgoingEdges.get(transactionId).contains(incompatible)) {
				outgoingEdges.get(transactionId).add(incompatible);
				newDependencies.add(incompatible);
			}
		}
		if (newDependencies.isEmpty())
			return false;

		HashSet<Integer> visited = new HashSet<Integer>();
		HashSet<Integer> onStack = new HashSet<Integer>();
		boolean cyclic = checkForCycle(transactionId, visited, onStack);

		if (cyclic) {
			outgoingEdges.get(transactionId).removeAll(newDependencies);
			return true;
		} else {
			for (Integer dependency : newDependencies) {
				if (incomingEdges.containsKey(dependency)) {
					incomingEdges.get(dependency).add(transactionId);
				} else {
					HashSet<Integer> hashSet = new HashSet<Integer>();
					hashSet.add(transactionId);
					incomingEdges.put(dependency, hashSet);
				}
			}
			return false;
		}
	}

	private boolean checkForCycle(Integer node, HashSet<Integer> visited,
			HashSet<Integer> onStack) {
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

	public void remove(int transactionId) {
		if (outgoingEdges.get(transactionId) != null) {
			for (Integer next : outgoingEdges.get(transactionId)) {
				incomingEdges.get(next).remove(transactionId);
			}
			outgoingEdges.remove(transactionId);
		}
		if (incomingEdges.get(transactionId) != null) {
			for (Integer previous : incomingEdges.get(transactionId)) {
				outgoingEdges.get(previous).remove(transactionId);
			}
			incomingEdges.remove(transactionId);
		}
	}

}