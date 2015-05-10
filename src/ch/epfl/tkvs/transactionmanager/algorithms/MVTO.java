package ch.epfl.tkvs.transactionmanager.algorithms;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.exceptions.CommitWithoutPrepareException;
import ch.epfl.tkvs.exceptions.TransactionAlreadyExistsException;
import ch.epfl.tkvs.exceptions.TransactionNotLiveException;
import ch.epfl.tkvs.exceptions.ValueDoesNotExistException;
import ch.epfl.tkvs.transactionmanager.Transaction;
import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.communication.TransactionTerminateMessage;
import ch.epfl.tkvs.transactionmanager.communication.requests.AbortRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.PrepareRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.MinAliveTransactionResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import ch.epfl.tkvs.transactionmanager.communication.utils.JSON2MessageConverter;
import ch.epfl.tkvs.transactionmanager.versioningunit.VersioningUnitMVTO;
import ch.epfl.tkvs.yarn.HDFSLogger;


public class MVTO extends CCAlgorithm {

    private VersioningUnitMVTO versioningUnit;
    private Set<Integer> primaryTransactions = new ConcurrentSkipListSet<Integer>();
    private Queue<Integer> primaryTerminated = new ConcurrentLinkedQueue<Integer>();
    private HDFSLogger log;
    
    public MVTO(RemoteHandler rh, HDFSLogger log) {
        super(rh, log);

        transactions = new ConcurrentHashMap<>();
        versioningUnit = VersioningUnitMVTO.getInstance();
        versioningUnit.init();
        this.log = log;
    }

    @Override
    public ReadResponse read(ReadRequest request) {
        int xid = request.getTransactionId();
        Serializable key = request.getEncodedKey();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new ReadResponse(new TransactionNotLiveException());
        }
        if (isLocalKey(request.getLocalityHash())) {
            Serializable value = versioningUnit.get(xid, key);
            if (value == null) {
                terminate(transaction, false);
                return new ReadResponse(new ValueDoesNotExistException());
            }
            return new ReadResponse((String) value);
        } else {
            return remote.read(transaction, request);
        }
    }

    @Override
    public GenericSuccessResponse write(WriteRequest request) {
        int xid = request.getTransactionId();
        Serializable key = request.getEncodedKey();
        Serializable value = request.getEncodedValue();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(new TransactionNotLiveException());
        }
        if (isLocalKey(request.getLocalityHash())) {
            try {
                versioningUnit.put(xid, key, value);
                return new GenericSuccessResponse();
            } catch (AbortException e) {
                terminate(transaction, false);
                return new GenericSuccessResponse(e);
            }
        } else {
            return remote.write(transaction, request);
        }
    }

    @Override
    public GenericSuccessResponse begin(BeginRequest request) {
        int xid = request.getTransactionId();

        // Transaction with duplicate id
        if (transactions.containsKey(xid)) {
            return new GenericSuccessResponse(new TransactionAlreadyExistsException());
        }
        Transaction t = new Transaction(xid);
        transactions.put(xid, t);
        versioningUnit.beginTransaction(xid);

        if (request.isPrimary()) {
        	primaryTransactions.add(xid);
        }
        
        return new GenericSuccessResponse();
    }

    @Override
    public GenericSuccessResponse commit(CommitRequest request) {
        int xid = request.getTransactionId();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(new TransactionNotLiveException());
        }

        if (!transaction.isPrepared) {
            return new GenericSuccessResponse(new CommitWithoutPrepareException());
        }
        terminate(transaction, true);
        return new GenericSuccessResponse();

    }

    private ConcurrentHashMap<Integer, Transaction> transactions;
    
    // Does cleaning up after end of transaction
    private void terminate(Transaction transaction, boolean success) {
    	if (primaryTransactions.contains(transaction.transactionId)) {
    		primaryTerminated.add(transaction.transactionId);
    	}
    	
        log.info("Terminating transaction with status " + success, MVTO.class);
        if (success) {
            versioningUnit.commit(transaction.transactionId);
        } else {
            versioningUnit.abort(transaction.transactionId);
        }
        if (!success && !isLocalTransaction(transaction))
            remote.abortOthers(transaction);
        
        transactions.remove(transaction.transactionId);
        primaryTransactions.remove(new Integer(transaction.transactionId));
    }

    @Override
    public GenericSuccessResponse abort(AbortRequest request) {
        int xid = request.getTransactionId();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(new TransactionNotLiveException());
        }

        terminate(transaction, false);
        return new GenericSuccessResponse();
    }

    @Override
    public GenericSuccessResponse prepare(PrepareRequest request) {
        int xid = request.getTransactionId();

        Transaction transaction = transactions.get(xid);

        // Transaction not begun or already terminated
        if (transaction == null) {
            return new GenericSuccessResponse(new TransactionNotLiveException());
        }
        try {
            versioningUnit.prepareCommit(xid);
            transaction.isPrepared = true;
            return new GenericSuccessResponse();
        } catch (AbortException e) {
            terminate(transaction, false);
            return new GenericSuccessResponse(e);
        }
    }

    @Override
    public Transaction getTransaction(int xid) {
        return transactions.get(xid);
    }

    @Override
    public void checkpoint() {
    	
    	ArrayList<Integer> toSend = new ArrayList<Integer>();
    	Integer tid = null;
    	while ((tid = primaryTerminated.poll()) != null) {
    		toSend.add(tid);
    	}
    	
    	TransactionTerminateMessage tMessage = new TransactionTerminateMessage(toSend);
    	MinAliveTransactionResponse response = null;
    	
    	try {
			JSONObject json = TransactionManager.sendToAppMaster(tMessage, true);
			response = (MinAliveTransactionResponse) JSON2MessageConverter.parseJSON(json, MinAliveTransactionResponse.class);
		} catch (Exception e) {
			log.error(e, getClass());
		}
    	
    	if (response != null) {
    		versioningUnit.garbageCollector(response.getTransactionId());
    	}
    }
    

    
    private void sendTerminateMessage(int tid) {
    	try {
    		TransactionManager.sendToAppMaster(new TransactionTerminateMessage(tid), false);
    	} catch (IOException e) {
    		log.info(e.getMessage(), getClass());
    	}
    }

}
