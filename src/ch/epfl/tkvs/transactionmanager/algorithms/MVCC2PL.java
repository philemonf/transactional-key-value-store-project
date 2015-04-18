package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockType;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockingUnit;
import ch.epfl.tkvs.transactionmanager.versioningunit.VersioningUnit;
import static ch.epfl.tkvs.transactionmanager.lockingunit.LockCompatibilityTable.newCompatibilityList;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MVCC2PL implements Algorithm {

	private LockingUnit lockingUnit;
	private VersioningUnit versioningUnit;
	private DeadlockPreventionUnit deadlock;

	public MVCC2PL() {
		lockingUnit = LockingUnit.instance;

		HashMap<LockType, List<LockType>> lockCompatibility = new HashMap<>();
		lockCompatibility.put(Lock.READ_LOCK,
				newCompatibilityList(Lock.READ_LOCK, Lock.WRITE_LOCK));
		lockCompatibility.put(Lock.WRITE_LOCK,
				newCompatibilityList(Lock.READ_LOCK));
		lockingUnit.initWithLockCompatibilityTable(lockCompatibility);

		versioningUnit = VersioningUnit.instance;
		versioningUnit.init();

		deadlock = new DeadlockPreventionUnit();
		transactions = new ConcurrentHashMap<>();
	}

	@Override
	public ReadResponse read(ReadRequest request) {
		int xid = request.getTransactionId();
		Serializable key = request.getEncodedKey();

		Transaction transaction = transactions.get(xid);

		if (transaction == null) {
			return new ReadResponse(false, null);
		}
		Lock lock = Lock.READ_LOCK;
		if (deadlock.checkForDeadlock(xid, key, lock)) {
			transactions.remove(xid);
			return new ReadResponse(false, null);
		}

		lockingUnit.lock(key, lock);
		transaction.addLock(key, lock);
		Serializable value = versioningUnit.get(xid, key);

		return new ReadResponse(true, (String) value);

	}

	@Override
	public GenericSuccessResponse write(WriteRequest request) {
		int xid = request.getTransactionId();
		Serializable key = request.getEncodedKey();
		Serializable value = request.getEncodedValue();

		Transaction transaction = transactions.get(xid);

		if (transaction == null) {
			return new GenericSuccessResponse(false);
		}

		Lock lock = Lock.WRITE_LOCK;
		if (deadlock.checkForDeadlock(xid, key, lock)) {
			transactions.remove(xid);
			return new GenericSuccessResponse(false);
		}

		lockingUnit.lock(key, lock);
		transaction.addLock(key, lock);
		versioningUnit.put(xid, key, value);
		return new GenericSuccessResponse(true);

	}

	@Override
	public GenericSuccessResponse begin(BeginRequest request) {
		int xid = request.getTransactionId();
		if (transactions.contains(xid))
			return new GenericSuccessResponse(false);
		transactions.put(xid, new Transaction(xid));
		return new GenericSuccessResponse(true);
	}

	@Override
	public GenericSuccessResponse commit(CommitRequest request) {
		int xid = request.getTransactionId();

		Transaction transaction = transactions.get(xid);
		if (transaction == null) {
			return new GenericSuccessResponse(false);
		}

		for (Serializable key : transaction.getLockedKeys()) {

			if (transaction.getLocksForKey(key).contains(Lock.WRITE_LOCK)) {
				if (deadlock.checkForDeadlock(xid, key, Lock.COMMIT_LOCK)) {
					transactions.remove(xid);
					return new GenericSuccessResponse(false);
				}

				lockingUnit.promote(key, transaction.getLocksForKey(key),
						Lock.COMMIT_LOCK);
			}
		}
		versioningUnit.commit(xid);
		deadlock.deadLockHandlingAtCommit(transaction);
		for (Serializable key : transaction.getLockedKeys()) {
			for (LockType lock : transaction.getLocksForKey(key)) {
				lockingUnit.release(key, lock);
			}
		}
		transactions.remove(xid);
		return new GenericSuccessResponse(true);
	}

	private ConcurrentHashMap<Integer, Transaction> transactions;

	private static enum Lock implements LockType {

		READ_LOCK, WRITE_LOCK, COMMIT_LOCK
	}

	private class Transaction {

		private int transactionId;
		private HashMap<Serializable, List<LockType>> currentLocks;

		public void addLock(Serializable key, LockType type) {
			if (currentLocks.containsKey(key)) {
				currentLocks.get(key).add(type);
			} else {
				currentLocks.put(key,
						new LinkedList<LockType>(Arrays.asList(type)));
			}
		}

		public Set<Serializable> getLockedKeys() {
			return currentLocks.keySet();
		}

		public List<LockType> getLocksForKey(Serializable key) {
			return currentLocks.get(key);
		}

		public int getTransactionId() {
			return transactionId;
		}

		public Transaction(int transactionId) {
			this.transactionId = transactionId;
			currentLocks = new HashMap<>();
		}

	}

	private class DeadlockPreventionUnit {
		private DeadlockGraph graph;
		private ConcurrentHashMap<Serializable, HashMap<LockType, HashSet<Integer>>> heldLocks;

		public DeadlockPreventionUnit() {
			graph = new DeadlockGraph();
			heldLocks = new ConcurrentHashMap<Serializable, HashMap<LockType, HashSet<Integer>>>();
		}

		public synchronized boolean checkForDeadlock(int transactionId,
				Serializable key, Lock lockType) {
			if (heldLocks.contains(key)) {
				List<Lock> incompatiblelockTypes = new LinkedList<Lock>();
				switch (lockType) {
				case COMMIT_LOCK:
					incompatiblelockTypes.add(Lock.READ_LOCK);
				case WRITE_LOCK:
					incompatiblelockTypes.add(Lock.WRITE_LOCK);
				case READ_LOCK:
					incompatiblelockTypes.add(Lock.COMMIT_LOCK);
					break;
				}
				HashSet<Integer> incompatibleTransactions = new HashSet<Integer>();
				for (Lock incompatibleLockType : incompatiblelockTypes) {
					incompatibleTransactions.addAll(heldLocks.get(key).get(
							incompatibleLockType));
				}
				if (graph
						.isCyclicAfter(transactionId, incompatibleTransactions)) {
					return true;
				} else {
					heldLocks.get(key).get(lockType).add(transactionId);
					return false;
				}
			} else { // if no lock is acquired on the key before
				HashMap<LockType, HashSet<Integer>> hashMap = new HashMap<LockType, HashSet<Integer>>();
				hashMap.put(Lock.READ_LOCK, new HashSet<Integer>());
				hashMap.put(Lock.WRITE_LOCK, new HashSet<Integer>());
				hashMap.put(Lock.COMMIT_LOCK, new HashSet<Integer>());
				hashMap.get(lockType).add(transactionId);
				heldLocks.put(key, hashMap);
				return false;
			}
		}

		public synchronized void deadLockHandlingAtCommit(
				Transaction transaction) {
			for (Serializable key : transaction.getLockedKeys()) {
				for (LockType lockType : transaction.getLocksForKey(key)) {
					heldLocks.get(key).get(lockType)
							.remove(transaction.transactionId);
				}
			}
			graph.remove(transaction.transactionId);
			return;
		}
	}
}