package ch.epfl.tkvs.yarn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class RoutingTable implements Serializable {

	
	
    private final static long serialVersionUID = 1;

    private final String AM_IP;
    private final int AM_PORT;
    private List<RemoteTransactionManager> tms;

    public RoutingTable(String AM_IP, int AM_PORT) {
        this.AM_IP = AM_IP;
        this.AM_PORT = AM_PORT;
        tms = new ArrayList<RemoteTransactionManager>();
    }

    public void addTM(RemoteTransactionManager tm) {
        tms.add(tm);
        tms.sort(new Comparator<RemoteTransactionManager>() {

    		@Override
    		public int compare(RemoteTransactionManager o1,
    				RemoteTransactionManager o2) {
    			return 0;
    		}

    	});
    }
    
    public RemoteTransactionManager findTM(int localityHash) {
    	if (tms.isEmpty()) {
    		throw new IllegalStateException("findTM called on an empty routing table");
    	}
    	
    	return tms.get(localityHash % tms.size());
    }

    public List<RemoteTransactionManager> getTMs() {
        return Collections.unmodifiableList(tms);
    }

    public String getAMIp() {
        return AM_IP;
    }

    public int getAMPort() {
        return AM_PORT;
    }

    public int size() {
        return tms.size();
    }

    public boolean contains(String tmIp) {
        for (RemoteTransactionManager tm : getTMs()) {
        	if (tm.getHostname().equals(tmIp)) {
        		return true;
        	}
        }
        
        return false;
    }
}
