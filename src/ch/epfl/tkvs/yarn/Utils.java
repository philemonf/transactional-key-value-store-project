package ch.epfl.tkvs.yarn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * The Utils class contains useful utilities throughout the lifetime of the YARN application.
 * @see ch.epfl.tkvs.yarn.Client
 * @see ch.epfl.tkvs.yarn.appmaster.AppMaster
 */
public class Utils {

    private static final boolean ENABLE_LOG = true;

    public static final int AM_MEMORY = 4096;
    public static final int AM_CORES = 8;
    public static final int TM_MEMORY = 4096;
    public static final int TM_CORES = 8;

    public static final String TKVS_JAR_NAME = "TKVS.jar";
    public static final Path TKVS_CONFIG_PATH = new Path("hdfs:///projects/transaction-manager/config/");
    public static final Path TKVS_JAR_PATH = new Path("hdfs:///projects/transaction-manager/" + TKVS_JAR_NAME);
    private static final Path AM_ADDRESS_PATH = new Path(Utils.TKVS_CONFIG_PATH, "AMAddress");

    public static void setUpEnv(Map<String, String> env, YarnConfiguration conf) {
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");
        // add the runtime classpath needed for tests to work
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }
        env.put("CLASSPATH", classPathEnv.toString());
        env.put("HADOOP_HOME", System.getenv("HADOOP_HOME"));
        // TODO: Add env variables if necessary for logging.
    }

    public static void setUpLocalResource(Path resPath, LocalResource res, YarnConfiguration conf) throws IOException {
        Path qPath = FileContext.getFileContext().makeQualified(resPath);

        FileStatus status = FileSystem.get(conf).getFileStatus(qPath);
        res.setResource(ConverterUtils.getYarnUrlFromPath(qPath));
        res.setSize(status.getLen());
        res.setTimestamp(status.getModificationTime());
        res.setType(LocalResourceType.FILE);
        res.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    /**
     * Reads the host names of the transaction master specified by the user in ./config/slaves.
     * @return an array list of those host names
     * @throws Exception in case of failure
     */
    public static ArrayList<String> readTMHostnames() throws Exception {
        ArrayList<String> tmHosts = new ArrayList<>();
        Path slavesPath = new Path(Utils.TKVS_CONFIG_PATH, "slaves");
        FileSystem fs = slavesPath.getFileSystem(new Configuration());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(slavesPath)));

        String line = reader.readLine();
        while (line != null && line.length() > 0 && !line.startsWith("#")) {
            tmHosts.add(NetUtils.normalizeHostName(line)); // Get IP.
            line = reader.readLine();
        }
        reader.close();
        fs.close();
        return tmHosts;
    }

    /**
     * Extract the IP from the host name format given by Hadoop.
     * @param addr - the raw host name
     * @return a cured host name
     */
    public static String extractIP(String addr) {
        int slashIndex = addr.lastIndexOf('/') + 1;
        if (slashIndex != -1) {
            addr = addr.substring(slashIndex);
        }
        int portIndex = addr.lastIndexOf(':');
        if (portIndex != -1) {
            addr = addr.substring(0, portIndex);
        }
        return addr;
    }

    /**
     * Read the concurrency control configuration in ./config/algorithm.
     * Possible config are: simple_2pl, mvcc2pl or mvto.
     * @return a String holding that config (the config of MVTO in case of failure)
     */
    public static String readAlgorithmConfig() {
		try {
			FileSystem fs = AM_ADDRESS_PATH.getFileSystem(new YarnConfiguration());
			Path algoConfigPath = new Path(Utils.TKVS_CONFIG_PATH, "algorithm");
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(algoConfigPath)));
	        String info = reader.readLine();
	        reader.close();
	        
	        return info;
	        
		} catch (Exception e) {
			return "mvto"; // default
		}
        
    }
    
    /**
     * Used by the AppMaster to write its address on HDFS.
     * @param address - the address of the AppMaster
     * @throws Exception - in case of failure
     */
    public static void writeAMAddress(String address) throws Exception {
        FileSystem fs = AM_ADDRESS_PATH.getFileSystem(new YarnConfiguration());
        PrintWriter pr = new PrintWriter(new OutputStreamWriter(fs.create(AM_ADDRESS_PATH, true)));
        pr.print(address);
        pr.close();
        fs.close();
    }

    /**
     * Used to read the AppMaster's address from HDFS.
     * @return the socket address of the AppMaster
     * @throws Exception - in case of failure
     */
    public static InetSocketAddress readAMAddress() throws Exception {
        FileSystem fs = AM_ADDRESS_PATH.getFileSystem(new YarnConfiguration());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(AM_ADDRESS_PATH)));
        String[] info = reader.readLine().split(":");
        reader.close();
        return new InetSocketAddress(info[0], Integer.parseInt(info[1]));
    }

    /**
     * Enable or disable the log. See ENABLE_LOG above.
     */
    public static void initLogLevel() {
        List<Logger> loggers = Collections.<Logger> list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for (Logger logger : loggers) {
            logger.setLevel(ENABLE_LOG ? Level.INFO : Level.WARN);
        }
    }

    public static void writeREPLHist(ArrayList<String> hist) throws Exception {
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(".hist"));
        oos.writeObject(hist);
        oos.close();
    }

    public static ArrayList<String> loadREPLHist() {
        ArrayList<String> res = new ArrayList<>();
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(".hist"));
            res.addAll((ArrayList<String>) ois.readObject());
            ois.close();
        } catch (Exception e) {
            // we dont care about missing file.
        }
        return res;
    }

    public static void writeAppId(ApplicationId id) throws Exception {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(".last_app_id")));
        writer.write(id.toString());
        writer.close();
    }
}
