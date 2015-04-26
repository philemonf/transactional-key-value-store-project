package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;


public enum VersioningUnitMVTO implements IVersioningUnit {
    instance;

    // testing-purpose only
    private int testing_max_xact = 0;

    // The Timestamp on which a Serializable key was last read
    private Map<Serializable, Integer> RTS;
    // The Timestamp on which a Serializable key was last written
    private Map<Serializable, Integer> WTS;
    // The different versions of a given key in descending order of timestamp
    private Map<Serializable, List<Version>> versions;

    private class Version {

        // key to access this version in the KVStore
        Serializable key;
        // Timestamp of this version's write
        int WTS;
    }

    @Override
    public void init() {
        // TODO Init and Flush KVStore ?
        KeyValueStore.instance.clear();

        // Flush data structures
        RTS = new ConcurrentHashMap<Serializable, Integer>();
        WTS = new ConcurrentHashMap<Serializable, Integer>();
    }

    /**
     * Tell the versioning unit about a new transaction
     * @param xid the ID or timestamp of the transaction If the xact is -1, then the VU should generate a xact (testing
     * purpose only)
     */
    public int begin_transaction(int xid) {

        // If we are testing the VU
        if (xid == -1) {
            return ++testing_max_xact;
        }

        // Initialize data structures for the new transaction
        // TODO

        return xid;
    }

    @Override
    public Serializable get(int xid, Serializable key) {
        return null;
    }

    @Override
    public void put(int xid, Serializable key, Serializable value) {
        // TODO Auto-generated method stub
    }

    @Override
    public void commit(int xid) {
        // TODO Auto-generated method stub
    }

    @Override
    public void stopNow() {
        // TODO Auto-generated method stub
    }

    public void abort(int xid) {
        // TODO Auto-generated method stub
    }

}
