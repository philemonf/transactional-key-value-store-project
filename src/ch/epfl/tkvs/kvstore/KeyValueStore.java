package ch.epfl.tkvs.kvstore;

/**
 * <Key,Value> Store
 */
public interface KeyValueStore {

    public void put(Key key, Value value);

    public Value get(Key key);

    public void update(Key key, Value value);

    public void delete(Key key);
}
