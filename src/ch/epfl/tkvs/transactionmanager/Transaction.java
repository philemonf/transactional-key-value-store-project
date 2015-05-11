package ch.epfl.tkvs.transactionmanager;

import java.util.HashMap;


/**
 * The super class used internally to represent a transaction. Algorithms may define sub classes. A primary transaction
 * is that which is running on the primary transaction manager (with which the user communicates). If the transaction is
 * distributed, the primary transaction manager communicates to other transaction managers to initiate secondary
 * transactions managed by .them.
 */
public class Transaction {

    public int transactionId; // id of transaction
    public boolean isPrepared; // is set when a prepare() completes successfully
    public boolean areAllRemoteAborted; // is set when abort messages are sent to all secondary transaction managers

    // maps locality hash of a remote transaction manager to whether the (secondary) version of this transaction running
    // in that transaction manager has successfully been prepared
    public HashMap<Integer, Boolean> remoteIsPrepared;

    public Transaction(int transactionId) {
        this.transactionId = transactionId;
        isPrepared = false;
        areAllRemoteAborted = false;
        remoteIsPrepared = new HashMap<>();
    }
}
