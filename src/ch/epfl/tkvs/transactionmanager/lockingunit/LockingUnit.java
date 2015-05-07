package ch.epfl.tkvs.transactionmanager.lockingunit;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.exceptions.DeadlockException;

/**
 * Locking Unit Singleton Call a function with LockingUnit.instance.fun(args)
 */
public enum LockingUnit {

	instance;

	private LockCompatibilityTable lct;
	private Map<Serializable, HashMap<LockType, List<Integer>>> locks = new HashMap<Serializable, HashMap<LockType, List<Integer>>>();
	private Map<Serializable, HashMap<LockType, Condition>> waitingLists = new HashMap<Serializable, HashMap<LockType, Condition>>();
	private Map<Integer, Condition> waitingTransactions = new HashMap<Integer, Condition>();
	private Set<Integer> transactionsToBeKilled = new HashSet<Integer>();
	private Lock internalLock = new ReentrantLock();
	private DeadlockGraph graph = new DeadlockGraph();

	private static Logger log = Logger.getLogger(LockingUnit.class.getName());

	/**
	 * MUST be called before use to specify the default 2PL lock compatibility
	 * table. To check whether lockTypeA is compatible with lockTypeB, the unit
	 * will do table.areCompatible(lockTypeA, lockTypeB).
	 * 
	 * For simplicity, please call this method before running the threads.
	 */
	public void init() {
		locks = new HashMap<Serializable, HashMap<LockType, List<Integer>>>();
		waitingLists = new HashMap<Serializable, HashMap<LockType, Condition>>();
		waitingTransactions = new HashMap<Integer, Condition>();
		transactionsToBeKilled = new HashSet<Integer>();
		graph = new DeadlockGraph();
		lct = new LockCompatibilityTable(false);
	}

	/**
	 * MUST be called before use to specify the default 2PL lock compatibility
	 * table. To check whether lockTypeA is compatible with lockTypeB, the unit
	 * will do table.areCompatible(lockTypeA, lockTypeB).
	 * 
	 * For simplicity, please call this method before running the threads.
	 */
	public void initOnlyExclusiveLock() {
		locks = new HashMap<Serializable, HashMap<LockType, List<Integer>>>();
		waitingLists = new HashMap<Serializable, HashMap<LockType, Condition>>();
		waitingTransactions = new HashMap<Integer, Condition>();
		transactionsToBeKilled = new HashSet<Integer>();
		graph = new DeadlockGraph();
		lct = new LockCompatibilityTable(true);
	}

	/**
	 * MUST be called before use to specify the lock compatibility table. By
	 * default, the lock compatibility table of 2PL is set. To check whether
	 * lockTypeA is compatible with lockTypeB, the unit will do
	 * table.areCompatible(lockTypeA, lockTypeB).
	 * 
	 * For simplicity, please call this method before running the threads.
	 * 
	 * @param table
	 *            the lock compatibility table - if null, use default parameter
	 */
	public void initWithLockCompatibilityTable(
			Map<LockType, List<LockType>> table) {
		locks = new HashMap<Serializable, HashMap<LockType, List<Integer>>>();
		waitingLists = new HashMap<Serializable, HashMap<LockType, Condition>>();
		waitingTransactions = new HashMap<Integer, Condition>();
		transactionsToBeKilled = new HashSet<Integer>();
		graph = new DeadlockGraph();
		if (table == null) {
			log.warn("LockCompatibilityTable is null. Using default compatibility table.");
			lct = new LockCompatibilityTable(false);
		} else {
			lct = new LockCompatibilityTable(table);
		}
	}

	/**
	 * Locks an object. Remember to init the module with the right lock
	 * compatibility table.
	 * 
	 * @param transactionID
	 *            ID of the transaction that requests the locks
	 * @param key
	 *            the key of the object to lock
	 * @param lockType
	 *            the lock type
	 * @throws Exception
	 */
	public void lock(int transactionID, Serializable key, LockType lockType)
			throws AbortException {
		try {
			internalLock.lock();
			while (!canLock(key, lockType)) {
				if (checkforDeadlock(transactionID, key, lockType)) {
					throw new DeadlockException();
				}
				waitOn(transactionID, key, lockType);
				if (transactionsToBeKilled.contains(transactionID)) {
					transactionsToBeKilled.remove(transactionID);
					throw new DeadlockException();
				}
			}
			addLock(transactionID, key, lockType);
		} catch (InterruptedException e) {
			// TODO: something
			log.error("Shit happens...");
		} finally {
			waitingTransactions.remove(transactionID);
			internalLock.unlock();
		}
	}

	/**
	 * Promotes a lock on an object atomically.
	 * 
	 * @param transactionID
	 *            ID of the transaction that promotes its lock(s)
	 * @param key
	 *            the key of the object associated with the lock
	 * @param oldTypes
	 *            the lock types to promote
	 * @param newType
	 *            the new lock type
	 * @throws Exception
	 */
	public <T extends LockType> void promote(int transactionID,
			Serializable key, List<T> oldTypes, LockType newType)
			throws AbortException {
		try {
			internalLock.lock();
			if (locks.containsKey(key)) {
				HashMap<LockType, List<Integer>> theLocks = allLocksExcept(
						transactionID, key, oldTypes);

				if (!theLocks.isEmpty()) {
					while (!lct.areCompatible(newType, theLocks.keySet())) {
						if (checkforDeadlock(transactionID, key, newType)) {
							throw new DeadlockException();
						}

						waitOn(transactionID, key, newType);
						if (transactionsToBeKilled.contains(transactionID)) {
							transactionsToBeKilled.remove(transactionID);
							throw new DeadlockException();
						}
						// Recompute the copy of the locks to avoid bug
						theLocks = allLocksExcept(transactionID, key, oldTypes);
					}
				}
				if (oldTypes != null)
					for (LockType oldType : oldTypes) {
						removeLock(transactionID, key, oldType);
					}
			}
			waitingTransactions.remove(transactionID);
			addLock(transactionID, key, newType);
		} catch (InterruptedException e) {
			// TODO: something
			log.error("Shit happens...");
		} finally {
			waitingTransactions.remove(transactionID);
			internalLock.unlock();
		}
	}

	public DeadlockGraph getDeadlockGraph() {
		internalLock.lock(); //TODO
		DeadlockGraph graphCopy = graph.copyOutgoingEdges();
		internalLock.unlock();
		return graphCopy;
	}

	private <T extends LockType> HashMap<LockType, List<Integer>> allLocksExcept(
			int transactionID, Serializable key, List<T> locksToExclude) {

		HashMap<LockType, List<Integer>> theLocks = new HashMap<LockType, List<Integer>>();
		if (!locks.containsKey(key)) {
			return theLocks;
		}
		for (LockType lockType : lct.getLockTypes()) {
			theLocks.put(lockType,
					new LinkedList<Integer>(locks.get(key).get(lockType)));
		}
		if (locksToExclude != null)
			for (LockType lockType : locksToExclude) {
				theLocks.get(lockType).remove(new Integer(transactionID));
			}
		for (LockType lockType : lct.getLockTypes()) {
			if (theLocks.get(lockType).isEmpty()) {
				theLocks.remove(lockType);
			}
		}
		return theLocks;
	}

	/**
	 * Releases all locks held by a transaction and removes the transaction from
	 * Deadlock graph.
	 * 
	 * @param transactionID
	 *            ID of the transaction whose locks are to be released
	 * @param heldLocks
	 *            the map of key to list of lock types, be careful to init the
	 *            module with the right lock compatibility table.
	 */
	public void releaseAll(int transactionID,
			HashMap<Serializable, List<LockType>> heldLocks) {
		internalLock.lock();
		try {
			for (Serializable key : heldLocks.keySet()) {
				for (LockType lockType : heldLocks.get(key)) {
					removeLock(transactionID, key, lockType);
					signalOn(key, lockType);
				}
			}
			graph.removeTransaction(transactionID);
		} finally {
			internalLock.unlock();
		}
	}

	/**
	 * Interrupts all waiting threads from a particular transaction. Interrupted
	 * threads must throw AbortException
	 * 
	 * @param transactionID
	 *            the id of the transaction
	 * @return true if there is any thread waiting, false otherwise
	 */
	public boolean interruptWaitingLocks(int transactionID) {
		internalLock.lock();
		if (!waitingTransactions.containsKey(transactionID))
			return false;
		transactionsToBeKilled.add(transactionID);
		waitingTransactions.get(transactionID).signalAll();
		internalLock.unlock();
		return true;
	}

	private boolean canLock(Serializable key, LockType lockType) {
		if (locks.containsKey(key)) {
			for (LockType lt : locks.get(key).keySet()) {
				if (!locks.get(key).get(lt).isEmpty()
						&& !lct.areCompatible(lockType, lt)) {
					return false;
				}
			}
		}
		return true;
	}

	private void addLock(int transactionID, Serializable key, LockType lockType) {
		if (locks.containsKey(key)) {
			locks.get(key).get(lockType).add(transactionID);
		} else {
			HashMap<LockType, List<Integer>> hashMap = new HashMap<LockType, List<Integer>>();
			for (LockType lt : lct.getLockTypes()) {
				hashMap.put(lt, new LinkedList<Integer>());
			}
			hashMap.get(lockType).add(transactionID);
			locks.put(key, hashMap);
		}
	}

	private void removeLock(int transactionID, Serializable key,
			LockType lockType) {
		if (locks.containsKey(key)) {
			locks.get(key).get(lockType).remove(new Integer(transactionID));
		}
		// remove the key from locks if there is not a lock on it.
		for (LockType lt : locks.get(key).keySet()) {
			if (!locks.get(key).get(lt).isEmpty()) {
				return;
			}
		}
		locks.remove(key);
	}

	private void waitOn(Integer transactionID, Serializable key,
			LockType lockType) throws InterruptedException {
		HashMap<LockType, Condition> em = waitingLists.get(key);
		if (em == null) {
			waitingLists.put(key, new HashMap<LockType, Condition>());
			em = waitingLists.get(key);
		}
		if (!em.containsKey(lockType)) {
			em.put(lockType, internalLock.newCondition());
		}
		waitingTransactions.put(transactionID, em.get(lockType));
		em.get(lockType).await();
	}

	private void signalOn(Serializable key, LockType lockType) {
		HashMap<LockType, Condition> em = waitingLists.get(key);
		if (em == null) {
			return;
		}
		for (LockType otherLockType : em.keySet()) {
			if (!lct.areCompatible(lockType, otherLockType)) {
				em.get(otherLockType).signalAll();
			}
		}
	}

	/**
	 * Checks if requesting for a new lock causes deadlock
	 * 
	 * @param transactionID
	 *            ID of the transaction requesting new lock
	 * @param key
	 *            key on which the lock is requested
	 * @param lockType
	 *            the type of the new lock requested
	 * @return
	 */
	private boolean checkforDeadlock(int transactionID, Serializable key,
			LockType lockType) {
		if (locks.containsKey(key)) {
			HashSet<Integer> incompatibleTransactions = getIncompatibleTransactions(
					transactionID, key, lockType);
			if (graph.isCyclicAfter(transactionID, incompatibleTransactions)) {
				return true;
			} else {
				return false;
			}
		} else { // if no lock is acquired on the key before
			return false;
		}
	}

	/**
	 * Gets transactions holding locks that are incompatible with new lock
	 * requested by another transaction
	 * 
	 * @param transactionID
	 *            the id of the new transaction requesting lock
	 * @param key
	 *            the key on which lock is requested
	 * @param lockType
	 *            the type of the lock which is requested
	 * @return
	 */
	private HashSet<Integer> getIncompatibleTransactions(int transactionID,
			Serializable key, LockType lockType) {
		Set<LockType> incompatiblelockTypes = lct
				.getIncompatibleLocks(lockType);
		HashSet<Integer> incompatibleTransactions = new HashSet<Integer>();
		for (LockType incompatibleLockType : incompatiblelockTypes) {
			incompatibleTransactions.addAll(locks.get(key).get(
					incompatibleLockType));
		}
		incompatibleTransactions.remove(transactionID);
		return incompatibleTransactions;
	}
}
