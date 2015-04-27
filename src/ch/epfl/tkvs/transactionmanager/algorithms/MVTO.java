//package ch.epfl.tkvs.transactionmanager.algorithms;
//
//import ch.epfl.tkvs.transactionmanager.AbortException;
//import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
//import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
//import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
//import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
//import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
//import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
//import ch.epfl.tkvs.transactionmanager.versioningunit.VersioningUnit;
//import java.io.Serializable;
//import java.util.concurrent.ConcurrentHashMap;
//
//
//public class MVTO implements Algorithm {
//
//    private VersioningUnit versioningUnit;
//
//    public MVTO() {
//        transactions = new ConcurrentHashMap<>();
//    }
//
//    @Override
//    public ReadResponse read(ReadRequest request) {
//        int xid = request.getTransactionId();
//        Serializable key = request.getEncodedKey();
//
//        Transaction transaction = transactions.get(xid);
//
//        // Transaction not begun or already terminated
//        if (transaction == null) {
//            return new ReadResponse(false, null);
//        }
//
//        try {
//            Serializable value = versioningUnit.get(xid, key);
//            return new ReadResponse(true, (String) value);
//
//        } catch (AbortException e) {
//            terminate(transaction);
//            return new ReadResponse(false, null);
//        }
//
//    }
//
//    @Override
//    public GenericSuccessResponse write(WriteRequest request) {
//        int xid = request.getTransactionId();
//        Serializable key = request.getEncodedKey();
//        Serializable value = request.getEncodedValue();
//
//        Transaction transaction = transactions.get(xid);
//
//        // Transaction not begun or already terminated
//        if (transaction == null) {
//            return new GenericSuccessResponse(false);
//        }
//
//        try {
//            versioningUnit.put(xid, key, value);
//            return new GenericSuccessResponse(true);
//        } catch (AbortException e) {
//            terminate(transaction);
//            return new GenericSuccessResponse(false);
//        }
//    }
//
//    @Override
//    public GenericSuccessResponse begin(BeginRequest request) {
//        int xid = request.getTransactionId();
//
//        // Transaction with duplicate id
//
//        if (transactions.containsKey(xid)) {
//            return new GenericSuccessResponse(false);
//        }
//        transactions.put(xid, new Transaction(xid));
//
//        return new GenericSuccessResponse(true);
//    }
//
//    @Override
//    public GenericSuccessResponse commit(CommitRequest request) {
//        int xid = request.getTransactionId();
//
//        Transaction transaction = transactions.get(xid);
//
//        // Transaction not begun or already terminated
//        if (transaction == null) {
//            return new GenericSuccessResponse(false);
//        }
//        try {
//            versioningUnit.commit(xid);
//            terminate(transaction);
//            return new GenericSuccessResponse(true);
//        } catch (AbortException e) {
//            terminate(transaction);
//            return new GenericSuccessResponse(false);
//        }
//    }
//
//    private ConcurrentHashMap<Integer, Transaction> transactions;
//
//    // Does cleaning up after end of transaction
//
//    private void terminate(Transaction transaction) {
//
//        transactions.remove(transaction.transactionId);
//    }
//
//    private class Transaction {
//
//        int transactionId;
//
//        public Transaction(int transactionID) {
//            this.transactionId = transactionID;
//        }
//
//    }
// }
