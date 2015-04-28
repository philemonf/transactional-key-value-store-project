package ch.epfl.tkvs.config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

import ch.epfl.tkvs.yarn.Utils;


public class NetConfig {

    public static final int AM_DEFAULT_PORT = 31299;
    public static final int TM_DEFAULT_PORT = 31200;

    // TODO: With a map, only 1 container per machine is allowd.
    LinkedHashMap<String, Integer> tms = new LinkedHashMap<String, Integer>();
    ArrayList<String> hosts = new ArrayList<String>();

    public NetConfig() throws IOException {
        Path slavesPath = new Path(Utils.TKVS_CONFIG_PATH, "slaves");
        FileSystem fs = slavesPath.getFileSystem(new Configuration());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(slavesPath)));

        String line = reader.readLine();
        while (line != null) {
            if (line.length() > 0 && !line.startsWith("#")) {
                int indexOfColumn = line.indexOf(':');

                String host = line;
                Integer port = null;
                if (indexOfColumn > 0) {
                    host = line.substring(0, indexOfColumn);
                    try {
                        port = Integer.parseInt(line.substring(indexOfColumn + 1));
                    } catch (Exception e) {
                        port = TM_DEFAULT_PORT;
                    }
                }
                tms.put(host, port);
            }
            line = reader.readLine();
        }

        hosts.addAll(tms.keySet());
        reader.close();
        fs.close();
    }

    public Map<String, Integer> getTMs() {
    	LinkedHashMap<String, Integer> safeTMs = new LinkedHashMap<String, Integer>();
    	
    	for (String host: tms.keySet()) {
    		safeTMs.put(host, getPortForHost(host));
    	}
    	
        return safeTMs;
    }

    public int getPortForHost(String host) {
    	
    	if (tms.get(host) == null) {
    		return TM_DEFAULT_PORT;
    	}
    	
        return tms.get(host);
    }

    public Pair<String, Integer> getTMbyHash(int hash) {
        String host = hosts.get(hash);
        return new Pair<String, Integer>(host, getPortForHost(host));
    }

    /**
     * Remove some files put on HDFS at start up. At the moment, it includes:
     * - the file containing the AppMaster hostname
     * @throws Exception 
     */
    public void cleanUpOldRuns() throws Exception {
    	Path path = getAMHostNameConfigPath("");
        FileSystem fs = path.getFileSystem(new Configuration());
        
        if (fs.exists(path)) {
        	fs.delete(path, true);
        }
    }
    
    /**
     * At start up, the app master write its host name in a file.
     * This methods returns it. It is blocking.
     * @throws Exception 
     */
    public String waitForAppMasterHostname() throws Exception {
    	Path path = getAMHostNameConfigPath("");
        FileSystem fs = path.getFileSystem(new Configuration());
        
        // Active wait until the file gets created
        while (!fs.exists(path)) {
        	Log.info("Waiting for AppMaster hostname");
        	Thread.sleep(3000); // wait 3 s before trying again
        }
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        try {
        	return reader.readLine();
        } finally {
        	reader.close();
        }
    }
    
    public void writeAppMasterHostname(String hostname) throws Exception {
    	// Write a tmp file to avoid race condition on the file
    	Path path = getAMHostNameConfigPath("_tmp");
        FileSystem fs = path.getFileSystem(new Configuration());
        
        FSDataOutputStream dos = fs.create(path);
        try {
        	dos.writeBytes(hostname);
        	dos.hsync();
        } finally {
        	dos.close();
        }
        
        // Rename the file once it has content
        fs.rename(path, getAMHostNameConfigPath(""));
    }
    
    private Path getAMHostNameConfigPath(String suffix) {
    	return new Path(Utils.TKVS_CONFIG_PATH, "appMasterHostName" + suffix);
    }
}
