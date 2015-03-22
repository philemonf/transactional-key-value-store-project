package ch.epfl.tkvs.kvstore;

import java.util.HashMap;

/**
 * <Key,Value> Store
 */
public class KeyValueStore {

	public HashMap<Key, Value> store;

	public KeyValueStore() {
		store = new HashMap<Key, Value>();
	}

	public void put(Key key, Value value) {
		store.put(key, value);
	}

	public Value get(Key key) {
		return store.get(key);
	}

	public void update(Key key, Value value) {
		store.put(key, value);
	}

	public void remove(Key key) {
		store.remove(key);
	}
}
