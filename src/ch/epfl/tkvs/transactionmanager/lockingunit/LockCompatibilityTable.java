package ch.epfl.tkvs.transactionmanager.lockingunit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * A lock compatibility table that one should provide the LockingUnit with.
 */
public class LockCompatibilityTable {

    Map<LockType, List<LockType>> table;
    boolean exclusive = false;

    public LockCompatibilityTable(boolean exclusive) {
        if (exclusive) {
            this.exclusive = true;
        } else {
            table = new HashMap<LockType, List<LockType>>();
            table.put(LockType.Default.READ_LOCK, newCompatibilityList(LockType.Default.READ_LOCK));
            table.put(LockType.Default.WRITE_LOCK, new LinkedList<LockType>());
        }
    }

    public LockCompatibilityTable(Map<LockType, List<LockType>> table) {
        this.table = table;
    }

    public static ArrayList<LockType> newCompatibilityList(LockType... lt) {
        return new ArrayList<LockType>(Arrays.asList(lt));
    }

    /**
     * Returns true iff lock1 is compatible with lock2.
     *
     * @param l1 the first lock type
     * @param l2 the second lock type
     * @return true iff lock1 is compatible with lock2.
     */
    public boolean areCompatible(LockType l1, LockType l2) {
        if (exclusive || !table.containsKey(l1))
            return false;
        return table.get(l1).contains(l2);
    }

    /**
     * Returns true iff the lock is compatible with the list of locks.
     *
     * @param lock the lock
     * @param llist the list of locks
     * @return true iff the lock is compatible with the list.
     */
    public boolean areCompatible(LockType lock, List<LockType> llist) {
        if (exclusive || !table.containsKey(lock))
            return false;
        return table.get(lock).containsAll(llist);
    }

}