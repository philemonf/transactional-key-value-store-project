package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;


/**
 * Versioning Unit Singleton Call a function with
 * VersioningUnit.instance.fun(args)
 */

public enum VersioningUnit {
    instance;

    private static Logger log = Logger.getLogger(VersioningUnit.class.getName());

    final static int PRIMARY_CACHE = 0;
    KeyValueStore kvStore;

    private Map<Integer, Cache> caches = new HashMap<Integer, Cache>();
    private Cache primary = new Cache(PRIMARY_CACHE);
    private Cache tmpPrimary = null;

    public String get(int xid, String key) {
        log.info("Get xid: " + xid + "- key: " + key);
        Cache xactCache = caches.get(xid);

        if (xactCache != null) {
            String value = xactCache.get(key);
            if (value != null) {
                return value;
            }
        }

        if (tmpPrimary != null) {
            String value = tmpPrimary.get(key);
            if (value != null) {
                return value;
            }
        }

        return primary.get(key);
    }

    public void put(int xid, String key, String value) {
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

                for (String key : tmpPrimary.getWritenKeys()) {
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
