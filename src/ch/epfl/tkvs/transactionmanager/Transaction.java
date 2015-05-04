package ch.epfl.tkvs.transactionmanager;

import java.util.HashMap;


public class Transaction {

    public int transactionId;
    public boolean isPrepared;
    public boolean areAllRemoteAborted;
    public HashMap<Integer, Boolean> remoteIsPrepared;

    public Transaction(int transactionId) {
        this.transactionId = transactionId;
        isPrepared = false;
        areAllRemoteAborted = false;
        remoteIsPrepared = new HashMap<>();
    }

}
