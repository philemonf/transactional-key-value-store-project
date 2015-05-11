/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.epfl.tkvs.transactionmanager;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import ch.epfl.tkvs.transactionmanager.lockingunit.LockType;


public class Transaction_2PL extends Transaction {

    private HashMap<Serializable, List<LockType>> currentLocks;

    public Transaction_2PL(int transactionId) {
        super(transactionId);

        currentLocks = new HashMap<>();
    }

    /**
     * Replaces old locks for a key with new locks
     *
     * @param key
     * @param newLocks
     */
    public void setLock(Serializable key, List<LockType> newLocks) {
        currentLocks.put(key, newLocks);
    }

    /**
     * Adds lock of type lockType for a key
     *
     * @param key
     * @param lockType
     */
    public void addLock(Serializable key, LockType lockType) {
        if (currentLocks.containsKey(key)) {
            currentLocks.get(key).add(lockType);
        } else {
            currentLocks.put(key, new LinkedList<LockType>(Arrays.asList(lockType)));
        }
    }

    /**
     * Checks if a particular lock is held for a key by this transaction
     * @param key The key for which lock is to be checked
     * @param lockType The type of the lock to be checked for the key
     * @return
     */
    public boolean checkLock(Serializable key, LockType lockType) {
        return currentLocks.get(key) != null && currentLocks.get(key).contains(lockType);
    }

    /**
     * 
     * @return The set of keys that are locked by this transaction
     */
    public Set<Serializable> getLockedKeys() {
        return currentLocks.keySet();
    }

    /**
     * 
     * @param key
     * @return Returns a list of locks held for a key
     */
    public List<LockType> getLocksForKey(Serializable key) {
        return currentLocks.get(key);
    }

    public HashMap<Serializable, List<LockType>> getCurrentLocks() {
        return currentLocks;
    }

}
