package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;


public class Cache {

    private int xid;
    private String prefix;
    private Set<Serializable> writtenKeys;

    public Cache(int xid) {
        this.xid = xid;
        this.prefix = "Cache" + xid + "_";
        this.writtenKeys = new HashSet<Serializable>();
    }

    public int getXid() {
        return xid;
    }

    public Serializable get(Serializable key) {
        return KeyValueStore.instance.get(prefixKey(key));
    }

    public void put(Serializable key, Serializable value) {
        KeyValueStore.instance.put(prefixKey(key), value);
        writtenKeys.add(key);
    }

    private Serializable prefixKey(Serializable key) {
        return new PrefixedKey(prefix, key);
    }

    public HashSet<Serializable> getWrittenKeys() {
        return new HashSet<Serializable>(writtenKeys);
    }

    @Override
    public String toString() {
        return prefix + getWrittenKeys();
    }

}
