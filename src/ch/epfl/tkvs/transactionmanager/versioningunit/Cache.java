package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;


/**
 * A cache is a set of keys written by a particular transaction.
 */
public class Cache {

    private int xid;
    private String prefix;
    private Set<Serializable> writtenKeys;

    /**
     * Key for the key-value store that is prefixed in order to be unique for a particular transaction and version
     */
    class PrefixedKey implements Serializable {

        private static final long serialVersionUID = 7099912040047138926L;
        private String prefix;
        private Serializable key;

        public PrefixedKey(String prefix, Serializable key) {
            this.prefix = prefix;
            this.key = key;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            PrefixedKey other = (PrefixedKey) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            if (prefix == null) {
                if (other.prefix != null)
                    return false;
            } else if (!prefix.equals(other.prefix))
                return false;
            return true;
        }

        private Cache getOuterType() {
            return Cache.this;
        }

    }

    /**
     * Create a cache for the given transaction ID
     * 
     * @param xid the ID of the transaction
     */
    public Cache(int xid) {
        this.xid = xid;
        this.prefix = "Cache" + xid + "_";
        this.writtenKeys = new HashSet<Serializable>();
    }

    public int getXid() {
        return xid;
    }

    /**
     * Retrieve the version of the given key in this cache
     * 
     * @param key the object to retrieve
     * @return the value of the given key in this cache
     */
    public Serializable get(Serializable key) {
        return KeyValueStore.instance.get(prefixKey(key));
    }

    /**
     * Write a key in this cache
     * 
     * @param key the key to write
     * @param value the value to write for the key
     */
    public void put(Serializable key, Serializable value) {
        KeyValueStore.instance.put(prefixKey(key), value);
        writtenKeys.add(key);
    }

    private Serializable prefixKey(Serializable key) {
        return new PrefixedKey(prefix, key);
    }

    /**
     * @return all keys written in this cache
     */
    public HashSet<Serializable> getWrittenKeys() {
        return new HashSet<Serializable>(writtenKeys);
    }

    @Override
    public String toString() {
        return prefix + getWrittenKeys();
    }

}