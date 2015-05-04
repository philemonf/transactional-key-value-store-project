package ch.epfl.tkvs.transactionmanager;

import java.util.HashMap;


public class Transaction {

    public int transactionId;
    public boolean isPrepared;
    public HashMap<Integer, Boolean> remoteIsPrepared;

    public Transaction(int transactionId) {
        this.transactionId = transactionId;
        isPrepared = false;
        remoteIsPrepared = new HashMap<>();
    }

}
