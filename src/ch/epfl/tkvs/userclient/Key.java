package ch.epfl.tkvs.userclient;

/**
 * The Key type for the <Key, Value> store
 */
public abstract class Key {

    @Override
    abstract public String toString();
    
    public abstract int getHash();
}