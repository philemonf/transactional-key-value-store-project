package ch.epfl.tkvs.keyvaluestore;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;


/**
 * <Key,Value> Store
 */
public enum KeyValueStore {
    instance;

    public ConcurrentHashMap<Serializable, Serializable> store = new ConcurrentHashMap<Serializable, Serializable>();

    public void clear() {
        store.clear();
    }

    public void put(Serializable key, Serializable value) {
        store.put(key, value);
    }

    public Serializable get(Serializable key) {
        return store.get(key);
    }

    public void remove(Serializable key) {
        store.remove(key);
    }
}
