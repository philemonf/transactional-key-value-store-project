/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author sachin
 */
public class MVCC2PL implements Algorithm {

	private LockingUnit lockingUnit;
	private VersioningUnit versioningUnit;

	public MVCC2PL() {
		versioningUnit = VersioningUnit.instance;
		lockingUnit = LockingUnit.instance;
		transactions = new ConcurrentHashMap<>();
	}

	boolean checkForDeadlock(int transactionId, String key, LockType lockType) {
		return false;
	}

	void deadLockHandlingatCommit(int transactionId) {

	}

	@Override
	public ReadResponse read(ReadRequest request) {
		int xid = request.getTransactionId();
		String key = request.getEncodedKey();

		Transaction transaction = transactions.get(xid);

		if (transaction == null) {
			return new ReadResponse(false, null);
		}
		LockType type = null; // TODO Add LockType
		if (checkForDeadlock(xid, key, type)) {
			return new ReadResponse(false, null);
		}

		// lockingUnit.lock(key,type); //TODO fix lock
		transaction.addLock(key, type);
		String value = (String) versioningUnit.get(xid, key);
		return new ReadResponse(true, value);

	}

	@Override
	public GenericSuccessResponse write(WriteRequest request) {
		int xid = request.getTransactionId();
		String key = request.getEncodedKey();
		String value = request.getEncodedValue();

		Transaction transaction = transactions.get(xid);

		if (transaction == null) {
			return new GenericSuccessResponse(false);
		}
		LockType type = null; // TODO Add LockType
		if (checkForDeadlock(xid, key, type)) {
			return new GenericSuccessResponse(false);
		}

		// lockingUnit.lock(key,type); //TODO fix lock
		transaction.addLock(key, type);
		versioningUnit.put(xid, key, value);
		return new GenericSuccessResponse(true);

	}

	@Override
	public GenericSuccessResponse begin(BeginRequest request) {
		int xid = request.getTransactionId();
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

		for (Key_LockType KL : transaction.getHeldLocks()) {
			// if(KL.type == WRITELOCK) //TODO Fix LockType
			// lockingUnit.lock(key, COMMITLOCK); //TODO Fix LockType
		}
		versioningUnit.commit(xid);
		deadLockHandlingatCommit(xid);
		for (Key_LockType KL : transaction.getHeldLocks()) {
			// lockingUnit.release(KL.key, KL.type); //TODO Fix LockType
		}
		return new GenericSuccessResponse(true);
	}

	private ConcurrentHashMap<Integer, Transaction> transactions;

	private class Transaction {

		private int transactionId;
		private LinkedList<Key_LockType> heldLocks;

		public void addLock(String key, LockType type) {
			heldLocks.add(new Key_LockType(key, type));
			// TODO check redundancy
		}

		public LinkedList<Key_LockType> getHeldLocks() {
			return heldLocks;
		}

		public int getTransactionId() {
			return transactionId;
		}

		public Transaction(int transactionId) {
			this.transactionId = transactionId;
			heldLocks = new LinkedList<>();
		}

	}

	private class Key_LockType {

		String key;
		LockType type;

		public Key_LockType(String key, LockType type) {
			this.key = key;
			this.type = type;
		}

	}
}
