package ch.epfl.tkvs.transactionmanager.lockingunit;

import static junit.framework.TestCase.assertEquals;

import java.util.HashSet;

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
    public void testRemove() {
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
    public void testRemove1() {
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

}
