package ch.epfl.tkvs.keyvaluestore;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;


/**
 * <Key,Value> Store
 */
public class KeyValueStore {

    public ConcurrentHashMap<Serializable, Serializable> store;

    public KeyValueStore() {
        store = new ConcurrentHashMap<Serializable, Serializable>();
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
