package ch.epfl.tkvs.transactionmanager.versioningunit;

import ch.epfl.tkvs.kvstore.Key;
import ch.epfl.tkvs.kvstore.Value;
import java.sql.Timestamp;

/**
 * Versioning Unit Singleton
 * Call a function with VersioningUnit.instance.fun(args)
 */
public enum VersioningUnit {
    instance;

    /**
     * Creates a new version for the given key with the given value
     * and the current timestamp. 
     * @return timestamp
     */
    public Timestamp createNewVersion(Key key, Value newValue){
        System.out.println("A new version of a key has been created");
    }
    
    /**
     * @return the corresponding value of the key with the closest oldest timestamp
     */
    public Value getValue(Key key, TimeStamp timestamp) {
        System.out.println("Getting a <key, value> version");
    }
    
    /**
     * Commit the key at the given timestamp
     */
    public void commit(Key key, TimeStamp timestamp){
         System.out.println("A commit has been done");
    }
}
