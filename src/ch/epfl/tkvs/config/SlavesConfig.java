package ch.epfl.tkvs.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ch.epfl.tkvs.yarn.Utils;

public class SlavesConfig {
	
	private static final int AM_DEFAULT_PORT = 9999;
	private static final int TM_DEFAULT_PORT = 9996;
	
	List<String> tmHosts = new LinkedList<String>();
	List<Integer> tmPorts = new LinkedList<Integer>();
	
	public SlavesConfig() throws IOException {
		Path slavesPath = new Path(Utils.TKVS_CONFIG_PATH, "slaves");
        FileSystem fs = slavesPath.getFileSystem(new Configuration());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(slavesPath)));
        
        String line;
        do {
        	
        	
        	line = reader.readLine();
        	if (line != null && !line.startsWith("#") && line.length() > 0) {
        		int indexOfColumn = line.indexOf(':');
        		
        		String host = line;
        		Integer port = null;
        		if (indexOfColumn > 0) {
        			host = line.substring(0, indexOfColumn);
        			port = Integer.parseInt(line.substring(indexOfColumn + 1));
        		}
        		
        		tmHosts.add(host);
        		tmPorts.add(port);
        	}
        } while (line != null);
        
        reader.close();
        fs.close();
	}
	
	public String[] getHosts() {
		return tmHosts.toArray(new String[tmHosts.size()]);
	}
	
	public int getAppMasterPort() {
		return AM_DEFAULT_PORT;
	}
	
	public int getPortForTransactionManager(int no) {
		Integer port = tmPorts.get(no);
		
		if (port != null) {
			return port.intValue();
		} else {
			return TM_DEFAULT_PORT;
		}
	}
	
	
}
