package ch.epfl.tkvs.test.userclient;

import ch.epfl.tkvs.user.Key;

public class MyKey extends Key {

    String k;

    public MyKey(String k) {
        this.k = k;
    }

    @Override
    public int getHash() {
        return 0;
    }

    @Override
    public String toString() {
        return k;
    }

}
