package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;
import ch.epfl.tkvs.transactionmanager.AbortException;


public class VersioningUnitMVTO {

    // The key-value storage where versions are stored
    private KeyValueStore KVS = KeyValueStore.instance;

    // The Timestamp on which a Serializable key was last read
    private Map<Serializable, Integer> RTS;
    // The different versions of a given key in descending order of timestamp
    private Map<Serializable, List<Version>> versions;

    // Contains the transactions from which a given transaction has read
    private Map<Integer, Set<Integer>> readFromXacts;
    // Contains the uncommitted transactions
    private Set<Integer> uncommitted;
    // Contains the aborted transactions
    private Set<Integer> abortedXacts;

    // The objects a given transaction has written
    private Map<Integer, Set<Serializable>> writtenKeys;

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

    /** Unique instance of the VersioningUnitMVTO class */
    private static VersioningUnitMVTO instance = null;

    /**
     * Private constructor of the Singleton
     */
    private VersioningUnitMVTO() {
        // Exists only to defeat instantiation
    }

    /**
     * Double-checked locking method to return the unique object
     * @return singleton VersioningUnitMVTO
     */
    public static VersioningUnitMVTO getInstance() {
        if (instance == null) {
            synchronized (VersioningUnit.class) {
                if (instance == null) {
                    instance = new VersioningUnitMVTO();
                }
            }
        }
        return instance;
    }

    /**
     * You MUST first call this before any other methods
     */
    public synchronized void init() {
        // TODO Init and Flush KVStore ?
        KVS.clear();

        // Flush data structures
        RTS = new ConcurrentHashMap<Serializable, Integer>();
        versions = new ConcurrentHashMap<Serializable, List<Version>>();
        readFromXacts = new ConcurrentHashMap<Integer, Set<Integer>>();
        uncommitted = new HashSet<Integer>();
        abortedXacts = new HashSet<Integer>();

        writtenKeys = new ConcurrentHashMap<Integer, Set<Serializable>>();
    }

    /**
     * Tell the versioning unit about a new transaction You MUST call this before any other methods about a specific
     * transaction
     * @param xid the ID or timestamp of the transaction
     */
    public synchronized int beginTransaction(int xid) {
        // Initialize data structures for the new transaction
        uncommitted.add(xid);
        writtenKeys.put(xid, new HashSet<Serializable>());
        readFromXacts.put(xid, new HashSet<Integer>());

        return xid;
    }

    public synchronized Serializable get(int xid, Serializable key) {

        // Update RTS
        if (RTS.get(key) == null || xid > RTS.get(key)) {
            RTS.put(key, xid);
        }

        // If no version of this key has been written yet
        if (versions.get(key) == null) {
            return null;
        }

        // Read written version with largest timestamp older than xid
        for (Version v : versions.get(key)) {
            if (v.WTS <= xid) {
                if (v.WTS != xid) {
                    readFromXacts.get(xid).add(v.WTS);
                }
                return KVS.get(v.key);
            }
        }

        // The transaction wants to read a key that did not have any version at that time
        return null;
    }

    /**
     * @throws AbortException if the write is not possible
     */
    public synchronized void put(int xid, Serializable key, Serializable value) throws AbortException {

        // Is the write possible ?
        if (RTS.get(key) != null && xid < RTS.get(key)) {
            abort(xid);
            throw new AbortException("Abort xact " + xid + " as it wanted to write " + key + " with value " + value + " but RTS is " + RTS.get(key));
        }

        // The write is possible, create a new version
        // It can overwrite a previous version by the same xid
        Version newVersion = new Version(new PrefixedKey("Version" + xid, key), xid);
        KVS.put(newVersion.key, value);

        writtenKeys.get(xid).add(key);

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

    /**
     * You MUST call this before calling commit(xid) Can block until other transactions commit or abort
     * @throws AbortException if the commmit is not possible
     */
    public synchronized void prepareCommit(int xid) throws AbortException {

        if (abortedXacts.contains(xid)) {
            return;
        }

        // TODO: optimize the notification of the correct waiting transaction
        try {
            while (readFromXacts.get(xid) != null && !Collections.disjoint(uncommitted, readFromXacts.get(xid))) {
                wait();
            }
        } catch (InterruptedException e) {
            // TODO Handle the exception
            e.printStackTrace();
        }

        if (readFromXacts.get(xid) != null && !Collections.disjoint(abortedXacts, readFromXacts.get(xid))) {
            Set<Integer> causes = new HashSet<Integer>(abortedXacts);
            causes.retainAll(readFromXacts.get(xid));
            abort(xid);
            notifyAll();
            throw new AbortException("Abort xact " + xid + " as it wanted to commit but it has read" + " for transactions that have aborted: " + causes);
        }
    }

    /**
     * Real commit, you MUST call this after a successful call to prepareCommit(xid)
     */
    public synchronized void commit(int xid) {

        if (!uncommitted.contains(xid) || abortedXacts.contains(xid)) {
            return;
        }

        // Commit successful
        uncommitted.remove(xid);
        readFromXacts.remove(xid);
        writtenKeys.remove(xid);
        notifyAll();
        garbageCollector();
    }

    public synchronized void abort(int xid) {

        if (abortedXacts.contains(xid)) {
            return; // already aborted
        }

        abortedXacts.add(xid);
        uncommitted.remove(xid);
        readFromXacts.remove(xid);

        // Rollback everything that the xact read and wrote
        for (Serializable key : writtenKeys.get(xid)) {

            for (Iterator<Version> iterator = versions.get(key).iterator(); iterator.hasNext();) {
                Version version = iterator.next();
                if (version.WTS == xid) {
                    // TODO: break since we only have one version
                    KeyValueStore.instance.remove(version.key);
                    iterator.remove();
                }
            }
        }

        writtenKeys.remove(xid);
        garbageCollector();
    }

    public synchronized void garbageCollector() {

        if (uncommitted.isEmpty()) {
            abortedXacts.clear();
            return;
        }

        int minAliveXid = Collections.min(uncommitted);

        // Removes useless versions stored in KVStore
        for (Serializable key : versions.keySet()) {
            boolean shouldRemoveAllFromNow = false;
            for (Iterator<Version> iterator = versions.get(key).iterator(); iterator.hasNext();) {
                Version version = iterator.next();
                if (shouldRemoveAllFromNow) {
                    KVS.remove(version.key);
                    iterator.remove();
                } else if (version.WTS <= minAliveXid) {
                    if (!abortedXacts.contains(version.WTS) && !uncommitted.contains(version.WTS))
                        shouldRemoveAllFromNow = true;
                }
            }
        }

        // Removes useless abortedXacts
        List<Integer> listMinXactReadFrom = new ArrayList<Integer>();
        for (Integer xid : uncommitted) {
            if (!readFromXacts.get(xid).isEmpty()) {
                listMinXactReadFrom.add(Collections.min(readFromXacts.get(xid)));
            }
        }

        if (listMinXactReadFrom.isEmpty()) {
            abortedXacts.clear();
            return;
        }

        int minXactReadFrom = Collections.min(listMinXactReadFrom);

        for (Iterator<Integer> iterator = abortedXacts.iterator(); iterator.hasNext();) {
            Integer xid = iterator.next();
            if (xid < minXactReadFrom) {
                iterator.remove();
            }
        }
    }
}
