package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.util.HashSet;
import java.util.Set;


public class Cache {

    private String prefix;
    private Set<String> writenKeys;

    public Cache(int id) {
        this.prefix = "Cache" + id + "_";
        this.writenKeys = new HashSet<String>();
    }

    public String get(String key) {
        return VersioningUnit.instance.kvStore.get(prefixKey(key));
    }

    public void put(String key, String value) {
        VersioningUnit.instance.kvStore.put(prefixKey(key), value);
        writenKeys.add(key);
    }

    private String prefixKey(String key) {
        return prefix + key;
    }

    public Set<String> getWritenKeys() {
        return new HashSet<String>(writenKeys);
    }

}
