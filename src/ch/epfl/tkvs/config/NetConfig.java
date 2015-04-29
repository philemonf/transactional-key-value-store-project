package ch.epfl.tkvs.config;

import ch.epfl.tkvs.yarn.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;


public class NetConfig {

    private static final Path AM_ADDRESS_PATH = new Path(Utils.TKVS_CONFIG_PATH, "AMAddress");
    private static Logger log = Logger.getLogger(NetConfig.class.getName());

    public static ArrayList<String> getTMHostnames() {
        ArrayList<String> tmHosts = new ArrayList<String>();
        try {
            Path slavesPath = new Path(Utils.TKVS_CONFIG_PATH, "slaves");
            FileSystem fs = slavesPath.getFileSystem(new Configuration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(slavesPath)));

            String line = reader.readLine();
            while (line != null && line.length() > 0 && !line.startsWith("#")) {
                tmHosts.add(line);
                line = reader.readLine();
            }
            reader.close();
            fs.close();
        } catch (Exception e) {
            log.error("Could not read slaves file", e);
        }
        return tmHosts;
    }

    public static String getOnlyHostname(String fixMe) {
        int slashIndex = fixMe.lastIndexOf('/');
        if (slashIndex != -1) {
            fixMe = fixMe.substring(0, slashIndex);
        }
        return fixMe;
    }

    public static void setAMAddress(String address) {
        try {
            FileSystem fs = FileSystem.get(new YarnConfiguration());
            PrintWriter pr = new PrintWriter(new OutputStreamWriter(fs.create(AM_ADDRESS_PATH, true)));
            pr.println(address);
            pr.close();
            fs.close();
        } catch (Exception e) {
            log.error("Could not write AMAddress file", e);
        }
    }

    public InetSocketAddress getAMAddress() {
        try {
            FileSystem fs = FileSystem.get(new YarnConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(AM_ADDRESS_PATH)));
            String[] info = reader.readLine().split(":");
            reader.close();
            return new InetSocketAddress(info[0], Integer.parseInt(info[1]));
        } catch (Exception e) {
            log.error("Could not read AMAddress file", e);
        }
        return null;
    }
}
