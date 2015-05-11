package ch.epfl.tkvs.user;

import java.io.Serializable;


/**
 * The Key type for the <Key, Value> store
 */
public abstract class Key implements Serializable {

    private static final long serialVersionUID = -4611660993943867708L;

    @Override
    abstract public String toString();

    public abstract int getLocalityHash();
}