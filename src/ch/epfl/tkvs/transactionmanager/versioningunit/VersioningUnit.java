package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;


public enum VersioningUnit {
    instance;

    private static Logger log = Logger.getLogger(VersioningUnit.class.getName());

    final static int PRIMARY_CACHE = 0;
    KeyValueStore kvStore;

    private Map<Integer, Cache> caches = new ConcurrentHashMap<Integer, Cache>();
    private Cache primary = new Cache(PRIMARY_CACHE);
    private Cache tmpPrimary = null;

    public Serializable get(int xid, Serializable key) {
        log.info("Get xid: " + xid + "- key: " + key);
        Cache xactCache = caches.get(xid);

        if (xactCache != null) {
            Serializable value = xactCache.get(key);
            if (value != null) {
                return value;
            }
        }

        if (tmpPrimary != null) {
            Serializable value = tmpPrimary.get(key);
            if (value != null) {
                return value;
            }
        }

        return primary.get(key);
    }

    public void put(int xid, Serializable key, Serializable value) {
        log.info("Put xid: " + xid + "- key: " + key + "- value: " + value);
        Cache xactCache = caches.get(xid);

        if (xactCache == null) {
            xactCache = new Cache(xid);

            caches.put(xid, xactCache);
        }

        xactCache.put(key, value);

    }

    public boolean commit(final int xid) {
        log.info("Commit xid: " + xid);

        if (tmpPrimary != null) {
            return false;
        }
        tmpPrimary = caches.get(xid);
        if (tmpPrimary == null) {
            return true;
        }

        new Thread(new Runnable() {

            @Override
            public void run() {

                for (Serializable key : tmpPrimary.getWrittenKeys()) {
                    primary.put(key, tmpPrimary.get(key));
                }

                caches.remove(xid);
                tmpPrimary = null;
            }
        }).start();

        return true;

    }

    public void abort(int xid) {
        caches.remove(xid);
    }

}
