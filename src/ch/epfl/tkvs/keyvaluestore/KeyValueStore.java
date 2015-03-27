package ch.epfl.tkvs.keyvaluestore;

import java.util.HashMap;

import org.apache.hadoop.hdfs.util.ByteArray;


/**
 * <Key,Value> Store
 */
public class KeyValueStore {

    public HashMap<String, ByteArray> store;

    public KeyValueStore() {
        store = new HashMap<String, ByteArray>();
    }

    public void put(String key, byte[] value) {
        store.put(key, new ByteArray(value));
    }

    public byte[] get(String key) {
        return store.get(key).getBytes();
    }

    public void remove(String key) {
        store.remove(key);
    }
}
