package ch.epfl.tkvs.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ch.epfl.tkvs.yarn.Utils;


public class SlavesConfig {

    public static final int AM_DEFAULT_PORT = 31299;
    public static final int TM_DEFAULT_PORT = 31200;

    // TODO: With a map, only 1 container per machine is allowd.
    LinkedHashMap<String, Integer> tms = new LinkedHashMap<String, Integer>();
    ArrayList<String> hosts = new ArrayList<String>();

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
                    try {
                        port = Integer.parseInt(line.substring(indexOfColumn + 1));
                    } catch (Exception e) {
                        port = TM_DEFAULT_PORT;
                    }
                }
                tms.put(host, port);
            }
        } while (line != null);

        hosts.addAll(tms.keySet());
        reader.close();
        fs.close();
    }

    public LinkedHashMap<String, Integer> getTMs() {
        return tms;
    }

    public int getPortForHost(String host) {
        return tms.get(host);
    }

    public Pair<String, Integer> getTMbyHash(int hash) {
        String host = hosts.get(hash);
        return new Pair<String, Integer>(host, tms.get(host));
    }

}
