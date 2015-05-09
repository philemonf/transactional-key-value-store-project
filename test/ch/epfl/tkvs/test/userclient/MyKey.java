package ch.epfl.tkvs.test.userclient;

import ch.epfl.tkvs.user.Key;


public class MyKey extends Key {

    String key;
    int localityHash;

    public MyKey(String k, int localityHash) {
        this.key = k;
        this.localityHash = localityHash;
    }

    @Override
    public int getLocalityHash() {
        return localityHash;
    }

    @Override
    public String toString() {
        return "key=" + key + "  hash=" + localityHash;
    }

}
