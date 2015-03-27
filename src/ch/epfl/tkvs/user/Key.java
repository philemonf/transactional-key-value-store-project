package ch.epfl.tkvs.user;

import java.io.Serializable;

/**
 * The Key type for the <Key, Value> store
 */
public abstract class Key implements Serializable {

    @Override
    abstract public String toString();

    public abstract int getHash();
}