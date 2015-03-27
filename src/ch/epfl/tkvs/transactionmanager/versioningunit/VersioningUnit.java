package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.sql.Timestamp;

import org.apache.log4j.Logger;


/**
 * Versioning Unit Singleton Call a function with
 * VersioningUnit.instance.fun(args)
 */
public enum VersioningUnit {
    instance;

    private static Logger log = Logger.getLogger(VersioningUnit.class.getName());

    /**
     * Creates a new version for the given key with the given value and the
     * current timestamp.
     * 
     * @return timestamp
     */
    public Timestamp createNewVersion(String key, byte[] newValue) {
        log.info("A new version of a key has been created");
        return null;
    }

    /**
     * @return the corresponding value of the key with the closest oldest
     *         timestamp
     */
    public byte[] getValue(String key, Timestamp timestamp) {
        log.info("Getting a <key, value> version");
        return null;
    }

    /**
     * Commit the key at the given timestamp
     */
    public void commit(String key, Timestamp timestamp) {
        log.info("A commit has been done");
    }
}
