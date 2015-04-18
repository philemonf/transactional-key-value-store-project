package ch.epfl.tkvs.transactionmanager.algorithms;

import static junit.framework.TestCase.assertEquals;
import java.util.HashSet;

import org.junit.Test;


public class DeadlockGraphTest {

	@Test
	public void testIsCyclicAfter() {
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
				
		incompatibleTransactions = new HashSet<Integer>();
		incompatibleTransactions.add(2);
		result = dg.isCyclicAfter(0, incompatibleTransactions);
		assertEquals(false, result);
		
		incompatibleTransactions.add(1);
		result = dg.isCyclicAfter(0, incompatibleTransactions);
		assertEquals(true, result);
		
		incompatibleTransactions = new HashSet<Integer>();
		incompatibleTransactions.add(0);
		result = dg.isCyclicAfter(2, incompatibleTransactions);
		assertEquals(true, result);
		
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
		
		dg.remove(0);
		
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
		
		dg.remove(1);
		result = dg.isCyclicAfter(0, incompatibleTransactions);
		assertEquals(false, result);
		
		incompatibleTransactions = new HashSet<Integer>();
		incompatibleTransactions.add(0);
		result = dg.isCyclicAfter(1, incompatibleTransactions);
		assertEquals(true, result);
		
		incompatibleTransactions = new HashSet<Integer>();
		incompatibleTransactions.add(2);
		incompatibleTransactions.add(0);
		result = dg.isCyclicAfter(1, incompatibleTransactions);
		assertEquals(true, result);
		
		incompatibleTransactions = new HashSet<Integer>();
		incompatibleTransactions.add(1);
		result = dg.isCyclicAfter(2, incompatibleTransactions);
		assertEquals(false, result);
		
		incompatibleTransactions = new HashSet<Integer>();
		incompatibleTransactions.add(1);
		result = dg.isCyclicAfter(0, incompatibleTransactions);
		assertEquals(false, result);
		
		dg.remove(2);
		
		incompatibleTransactions = new HashSet<Integer>();
		incompatibleTransactions.add(0);
		result = dg.isCyclicAfter(1, incompatibleTransactions);
		assertEquals(true, result);
		
		dg.remove(0);
		dg.remove(1);
		dg.remove(2);
		dg.remove(3);
		
		//All clean
		
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
				
		incompatibleTransactions = new HashSet<Integer>();
		incompatibleTransactions.add(2);
		result = dg.isCyclicAfter(0, incompatibleTransactions);
		assertEquals(false, result);
		
		incompatibleTransactions.add(1);
		result = dg.isCyclicAfter(0, incompatibleTransactions);
		assertEquals(true, result);
		
		incompatibleTransactions = new HashSet<Integer>();
		incompatibleTransactions.add(0);
		result = dg.isCyclicAfter(2, incompatibleTransactions);
		assertEquals(true, result);
		
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
}
