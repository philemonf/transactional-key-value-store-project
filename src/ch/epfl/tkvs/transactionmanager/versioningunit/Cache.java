package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;


public class Cache {

    private int xid;
    private String prefix;
    private Set<Serializable> writtenKeys;

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

}
