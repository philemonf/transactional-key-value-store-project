package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;
import ch.epfl.tkvs.user.AbortException;


public enum VersioningUnitMVTO implements IVersioningUnit {
    instance;

    // The key-value storage where versions are stored
    private KeyValueStore KVS = KeyValueStore.instance;

    // The Timestamp on which a Serializable key was last read
    private Map<Serializable, Integer> RTS;
    // The different versions of a given key in descending order of timestamp
    private Map<Serializable, List<Version>> versions;

    // testing-purpose only
    private int testing_max_xact = 0;

    private class Version {

        // key to access this version in the KVStore
        Serializable key;
        // Timestamp of this version's write
        int WTS;

        public Version(Serializable key, int WTS) {
            this.key = key;
            this.WTS = WTS;
        }
    }

    @Override
    public synchronized void init() {
        // TODO Init and Flush KVStore ?
        KVS.clear();
        testing_max_xact = 0;

        // Flush data structures
        RTS = new ConcurrentHashMap<Serializable, Integer>();
        versions = new ConcurrentHashMap<Serializable, List<Version>>();
    }

    /**
     * Tell the versioning unit about a new transaction
     * @param xid the ID or timestamp of the transaction If the xact is -1, then the VU should generate a xact (testing
     * purpose only)
     */
    public synchronized int begin_transaction(int xid) {

        // If we are testing the VU
        if (xid == -1) {
            xid = ++testing_max_xact;
        }

        // Initialize data structures for the new transaction
        // TODO

        return xid;
    }

    @Override
    public synchronized Serializable get(int xid, Serializable key) {

        // Update RTS
        if (RTS.get(key) == null || xid > RTS.get(key)) {
            RTS.put(key, xid);
        }

        // If no version of this key has been written yet
        if (versions.get(key) == null) {
            return null;
        }

        // Read written verstion with largest timestamp older than xid
        for (Version v : versions.get(key)) {
            if (v.WTS <= xid) {
                return KVS.get(v.key);
            }
        }

        // The transaction wants to read a key that did not have any version at that time
        return null;
    }

    @Override
    public synchronized void put(int xid, Serializable key, Serializable value) throws AbortException {

        // Is the write possible ?
        if (RTS.get(key) != null && xid < RTS.get(key)) {
            throw new AbortException("Abort xact " + xid + " as it wanted to write " + key + " with value " + value + " but RTS is " + RTS.get(key));
        }

        // The write is possible, create a new version
        // It can overwrite a previous version by the same xid
        Version newVersion = new Version(new PrefixedKey("Version" + xid, key), xid);
        KVS.put(newVersion.key, value);

        // Insert it at the correct place in the version's list
        List<Version> listOfVersions = versions.get(key);
        if (listOfVersions == null) {
            versions.put(key, new ArrayList<Version>());
            versions.get(key).add(newVersion);
        } else {
            if (xid < listOfVersions.get(listOfVersions.size() - 1).WTS) {
                listOfVersions.add(newVersion);
            } else {
                for (int i = 0; i < listOfVersions.size(); i++) {
                    if (xid > listOfVersions.get(i).WTS) {
                        listOfVersions.add(i, newVersion);
                        break;
                    } else if (xid == listOfVersions.get(i).WTS) {
                        listOfVersions.add(i, newVersion);
                        listOfVersions.remove(i + 1);
                        break;
                    }
                }
            }
        }
    }

    @Override
    public synchronized void commit(int xid) {
        // TODO Auto-generated method stub
    }

    @Override
    public synchronized void stopNow() {
        // TODO Auto-generated method stub
    }

    public synchronized void abort(int xid) {
        // TODO Auto-generated method stub
    }

}
