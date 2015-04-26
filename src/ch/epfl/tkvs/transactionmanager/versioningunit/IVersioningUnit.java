package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.io.Serializable;


public interface IVersioningUnit {

    /**
     * MUST be called before first use. This initializes the module.
     */
    public void init();

    /**
     * Reads a key. The exact behavior depends on the underlying implementation
     * @param xid the current transaction (= Xact timestamp) doing the read
     * @param key the key to read
     * @return the value associated with the key
     */
    public Serializable get(int xid, Serializable key);

    /**
     * Write a new version for a given key
     * @param xid the current transaction (= Xact timestamp) doing the write
     * @param key the key to be written
     * @param value the value for the new version
     */
    public void put(int xid, Serializable key, Serializable value);

    /**
     * Commit the changes done by a transaction The transaction SHOULD NOT do any other requests to the VersioningUnit
     * @param xid the current transaction (=Xact timestamp) that wants to commit
     */
    public void commit(final int xid);

    /**
     * Abort the current transaction The transaction SHOULD NOT do any other requests
     * @param xid the transaction (= Xact timestamp) to be aborted
     */
    public void abort(int xid);

    /**
     * Stop gracefully the VersioningUnit Must be call if one wants to restart the VersioningUnit No need to call it if
     * the application is exiting
     */
    public void stopNow();

}
