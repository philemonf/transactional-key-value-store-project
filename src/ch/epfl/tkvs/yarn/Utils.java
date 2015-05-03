package ch.epfl.tkvs.yarn;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;


public class Utils {

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

    public static ArrayList<String> readTMHostnames() throws Exception {
        ArrayList<String> tmHosts = new ArrayList<>();
        Path slavesPath = new Path(Utils.TKVS_CONFIG_PATH, "slaves");
        FileSystem fs = slavesPath.getFileSystem(new Configuration());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(slavesPath)));

        String line = reader.readLine();
        while (line != null && line.length() > 0 && !line.startsWith("#")) {
            tmHosts.add(NetUtils.normalizeHostName(line));
            line = reader.readLine();
        }
        reader.close();
        fs.close();
        return tmHosts;
    }

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

    public static void writeAMAddress(String address) throws Exception {
        FileSystem fs = FileSystem.get(new YarnConfiguration());
        PrintWriter pr = new PrintWriter(new OutputStreamWriter(fs.create(AM_ADDRESS_PATH, true)));
        pr.print(address);
        pr.close();
        fs.close();
    }

    public static InetSocketAddress readAMAddress() throws Exception {
        FileSystem fs = FileSystem.get(new YarnConfiguration());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(AM_ADDRESS_PATH)));
        String[] info = reader.readLine().split(":");
        reader.close();
        return new InetSocketAddress(info[0], Integer.parseInt(info[1]));
    }
}
