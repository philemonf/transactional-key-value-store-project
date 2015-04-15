package ch.epfl.tkvs.transactionmanager.lockingunit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;


/**
 * A lock compatibility table that one should provide the LockingUnit with.
 */
public class LockCompatibilityTable_2 {

    HashMap<LockType, ArrayList<LockType>> table;
    boolean exclusive = false;

    public LockCompatibilityTable_2(boolean exclusive) {
        if (exclusive) {
            this.exclusive = true;
        } else {
            table = new HashMap<LockType, ArrayList<LockType>>();
            table.put(LockType.Default.READ_LOCK, newCompatibilityList(LockType.Default.READ_LOCK));
        }
    }
    
    public LockCompatibilityTable_2(HashMap<LockType, ArrayList<LockType>> table) {
        this.table = table;
    }

    public static ArrayList<LockType> newCompatibilityList(LockType...lt) {
        return new ArrayList<LockType>(Arrays.asList(lt));
    }

    public boolean areCompatible(LockType l1, LockType l2) {
        if (exclusive || !table.containsKey(l1))
            return false;
        for (LockType lt : table.get(l1)) {
            if (lt.equals(l2))
                return true;
        }
        return false;
    }

}