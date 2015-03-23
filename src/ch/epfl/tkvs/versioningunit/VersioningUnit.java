package ch.epfl.tkvs.versioningunit;

import java.sql.Timestamp;



/**
 * Versioning Unit Singleton Call a function with
 * VersioningUnit.instance.fun(args)
 */
public enum VersioningUnit {
    instance;

    /**
     * Creates a new version for the given key with the given value and the
     * current timestamp.
     * 
     * @return timestamp
     */
    public Timestamp createNewVersion(String key, byte[] newValue) {
        System.out.println("A new version of a key has been created");
        return null;
    }

    /**
     * @return the corresponding value of the key with the closest oldest
     *         timestamp
     */
    public byte[] getValue(String key, Timestamp timestamp) {
        System.out.println("Getting a <key, value> version");
        return null;
    }

    /**
     * Commit the key at the given timestamp
     */
    public void commit(String key, Timestamp timestamp) {
        System.out.println("A commit has been done");
    }
}
