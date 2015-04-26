package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public enum VersioningUnitMVTO implements IVersioningUnit {
    instance;
    
    // The Timestamp on which a Serializable key  was last read
    private Map<Serializable, Integer> RTS;
    // The Timestamp on which a Serializable key was last written
    private Map<Serializable, Integer> WTS;

    @Override
    public void init() {
        RTS = new ConcurrentHashMap<Serializable, Integer>();
        WTS = new ConcurrentHashMap<Serializable, Integer>();
    }

    @Override
    public Serializable get(int xid, Serializable key) {
        return null;
    }

    @Override
    public void put(int xid, Serializable key, Serializable value) {
        // TODO Auto-generated method stub

    }

    @Override
    public void commit(int xid) {
        // TODO Auto-generated method stub

    }

    @Override
    public void stopNow() {
        // TODO Auto-generated method stub

    }

    public int begin_transaction(int xact) {
        // TODO Auto-generated method stub
        return 0;
    }

    public void abort(int xact) {
        // TODO Auto-generated method stub
        
    }

}
