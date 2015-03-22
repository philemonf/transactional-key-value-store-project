package ch.epfl.tkvs.userclient;

import ch.epfl.tkvs.kvstore.Value;


public class MyValue extends Value {

    String val;

    public MyValue(String val) {
        this.val = val;
    }

    public String toString() {
        return val;
    }

}
