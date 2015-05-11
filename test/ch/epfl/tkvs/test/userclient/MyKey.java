package ch.epfl.tkvs.test.userclient;

import ch.epfl.tkvs.user.Key;


public class MyKey extends Key {

    private static final long serialVersionUID = -1420790611895983705L;

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
