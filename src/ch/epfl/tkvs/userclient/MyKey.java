package ch.epfl.tkvs.userclient;

import ch.epfl.tkvs.kvstore.Key;


public class MyKey extends Key {

    int k;

    public MyKey(int i) {
        k = i;
    }

    public int getKey() {
        return k;
    }

    public String toString() {
        return String.valueOf(k);
    }

}
