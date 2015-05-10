package ch.epfl.tkvs.transactionmanager.lockingunit;

import static junit.framework.TestCase.assertEquals;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

import org.junit.Test;


public class DeadlockGraphTest {

    @Test
    public void testIsCyclicAfter1() {
        DeadlockGraph dg = new DeadlockGraph();
        HashSet<Integer> incompatibleTransactions = new HashSet<Integer>();

        boolean result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions.add(0);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(1);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(true, result);
    }

    @Test
    public void testIsCyclicAfter2() {
        DeadlockGraph dg = new DeadlockGraph();
        HashSet<Integer> incompatibleTransactions = new HashSet<Integer>();

        boolean result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions.add(0);
        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions.add(1);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(true, result);
    }

    @Test
    public void testIsCyclicAfter3() {
        DeadlockGraph dg = new DeadlockGraph();
        HashSet<Integer> incompatibleTransactions = new HashSet<Integer>();

        boolean result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions.add(0);
        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(0);
        result = dg.isCyclicAfter(2, incompatibleTransactions);
        assertEquals(true, result);
    }

    @Test
    public void testIsCylicAfter4() {
        DeadlockGraph dg = new DeadlockGraph();
        HashSet<Integer> incompatibleTransactions = new HashSet<Integer>();

        boolean result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions.add(0);
        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(3);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(3, incompatibleTransactions);
        assertEquals(false, result);
    }

    @Test
    public void testRemove1() {
        DeadlockGraph dg = new DeadlockGraph();
        HashSet<Integer> incompatibleTransactions = new HashSet<Integer>();

        dg.removeTransaction(0);

        boolean result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions.add(0);
        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(1);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(true, result);

        dg.removeTransaction(1);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(0);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(true, result);

        dg.removeTransaction(1);
        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(2);
        incompatibleTransactions.add(0);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(false, result);

        dg.removeTransaction(1);
        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(1);
        result = dg.isCyclicAfter(2, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(1);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        dg.removeTransaction(2);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(0);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(true, result);

        dg.removeTransaction(0);
        dg.removeTransaction(1);
        dg.removeTransaction(2);
        dg.removeTransaction(3);

        // All clean
        incompatibleTransactions = new HashSet<Integer>();
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions.add(0);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(1);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(true, result);
    }

    @Test
    public void testRemove2() {
        DeadlockGraph dg = new DeadlockGraph();
        HashSet<Integer> incompatibleTransactions = new HashSet<Integer>();

        incompatibleTransactions = new HashSet<Integer>();
        boolean result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions.add(0);
        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(1, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(2);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(false, result);

        incompatibleTransactions.add(1);
        result = dg.isCyclicAfter(0, incompatibleTransactions);
        assertEquals(true, result);
    }

    @Test
    public void testMergeConstructor1() {
        DeadlockGraph dg1 = new DeadlockGraph();
        DeadlockGraph dg2 = new DeadlockGraph();

        HashSet<Integer> incompatibleTransactions = new HashSet<Integer>();

        incompatibleTransactions.add(2);
        incompatibleTransactions.add(3);
        dg1.addDependencies(1, incompatibleTransactions);

        incompatibleTransactions.clear();
        incompatibleTransactions.add(1);
        incompatibleTransactions.add(3);
        dg2.addDependencies(2, incompatibleTransactions);

        LinkedList<DeadlockGraph> graphs = new LinkedList<DeadlockGraph>();
        graphs.add(dg1);
        graphs.add(dg2);
        DeadlockGraph merged12 = new DeadlockGraph(graphs);

        incompatibleTransactions.clear();
        incompatibleTransactions.add(4);
        assertEquals(true, merged12.isCyclicAfter(1, incompatibleTransactions));
    }

    @Test
    public void testMergeConstructor2() {
        DeadlockGraph dg1 = new DeadlockGraph();
        DeadlockGraph dg2 = new DeadlockGraph();
        DeadlockGraph expectedGraph = new DeadlockGraph();
        LinkedList<DeadlockGraph> graphs = new LinkedList<DeadlockGraph>();

        DeadlockGraph merged12 = new DeadlockGraph(graphs);
        assertEquals(true, merged12.equals(expectedGraph));

        graphs.add(dg1);
        graphs.add(dg2);
        merged12 = new DeadlockGraph(graphs);
        assertEquals(true, merged12.equals(expectedGraph));

        HashSet<Integer> incompatibleTransactions = new HashSet<Integer>();
        incompatibleTransactions.add(3);
        dg1.addDependencies(1, incompatibleTransactions);
        dg2.addDependencies(2, incompatibleTransactions);
        merged12 = new DeadlockGraph(graphs);
        // System.out.println(merged12);

        expectedGraph.addDependencies(1, incompatibleTransactions);
        expectedGraph.addDependencies(2, incompatibleTransactions);
        assertEquals(true, merged12.equals(expectedGraph));

        incompatibleTransactions.clear();
        incompatibleTransactions.add(1);
        dg1.addDependencies(2, incompatibleTransactions);
        expectedGraph.addDependencies(2, incompatibleTransactions);
        incompatibleTransactions.clear();
        incompatibleTransactions.add(2);
        dg2.addDependencies(1, incompatibleTransactions);
        expectedGraph.addDependencies(1, incompatibleTransactions);
        merged12 = new DeadlockGraph(graphs);
        // System.out.println(merged12);
        assertEquals(true, merged12.equals(expectedGraph));

        incompatibleTransactions.clear();
        incompatibleTransactions.add(1);
        dg2.addDependencies(2, incompatibleTransactions);
        incompatibleTransactions.clear();
        incompatibleTransactions.add(2);
        dg1.addDependencies(1, incompatibleTransactions);
        merged12 = new DeadlockGraph(graphs);
        // System.out.println(merged12);
        assertEquals(true, merged12.equals(expectedGraph));
    }

    @Test
    public void testCheckForCycles() {
        DeadlockGraph graph = new DeadlockGraph();
        HashSet<Integer> incompatibleTransactions = new HashSet<Integer>();
        Set<Integer> transactionsKilled = null;

        Random randomGenerator = new Random();
        while (transactionsKilled == null) {
            for (int i = 0; i < 5; i++) {
                incompatibleTransactions.clear();
                for (int j = 0; j < 3; j++) {
                    incompatibleTransactions.add(randomGenerator.nextInt(10));
                }
                int transactionID;
                do {
                    transactionID = randomGenerator.nextInt(10);
                } while (incompatibleTransactions.contains(transactionID));
                graph.addDependencies(transactionID, incompatibleTransactions);
            }
            transactionsKilled = graph.checkForCycles();
        }

        incompatibleTransactions.clear();
        incompatibleTransactions.add(10);
        graph.addDependencies(10, incompatibleTransactions);
        // System.out.println(graph);
        transactionsKilled = graph.checkForCycles();
        assertEquals(true, transactionsKilled.size() == 1);
        assertEquals(true, transactionsKilled.contains(10));
        assertEquals(true, graph.checkForCycles().size() == 0);
    }
}
