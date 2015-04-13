package ch.epfl.tkvs.keyvaluestore;

import java.util.HashMap;


/**
 * <Key,Value> Store
 */
public class KeyValueStore {

    public HashMap<String, String> store;

    public KeyValueStore() {
        store = new HashMap<String, String>();
    }

    public void put(String key, String value) {
        store.put(key, value);
    }

    public String get(String key) {
        return store.get(key);
    }

    public void remove(String key) {
        store.remove(key);
    }
}
