package ch.epfl.tkvs.transactionmanager.lockingunit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
			table.put(LockType.Default.READ_LOCK,
					newCompatibilityList(LockType.Default.READ_LOCK));
		}
	}

	public LockCompatibilityTable(Map<LockType, List<LockType>> table) {
		this.table = table;
	}

	public static ArrayList<LockType> newCompatibilityList(LockType... lt) {
		return new ArrayList<LockType>(Arrays.asList(lt));
	}

	public Set<LockType> getIncompatibleLocks(LockType l1) {
		List<LockType> compatibleLocks = table.get(l1);
		Set<LockType> incompatibleLocks = new HashSet<LockType>();
		for (LockType l2 : table.keySet()) {
			if (!compatibleLocks.contains(l2)) {
				incompatibleLocks.add(l2);
			}
		}
		return incompatibleLocks;
	}

	public Set<LockType> getLockTypes() {
		return table.keySet();
	}
	
	/**
	 * Returns true iff lock1 is compatible with lock2.
	 * 
	 * @param l1
	 *            the first lock type
	 * @param l2
	 *            the second lock type
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
	 * @param lock
	 *            the lock
	 * @param llist
	 *            the list of locks
	 * @return true iff the lock is compatible with the list.
	 */
	public boolean areCompatible(LockType lock, Collection<LockType> llist) {
		if (llist.isEmpty())
			return true;
		if (exclusive || !table.containsKey(lock))
			return false;
		return table.get(lock).containsAll(llist);
	}

}