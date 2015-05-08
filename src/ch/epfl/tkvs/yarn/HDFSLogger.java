package ch.epfl.tkvs.yarn;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;


//TODO: In the future, use log aggregation instead!
public class HDFSLogger {

    private final boolean ALSO_USE_LOG4J = false;
    private final String TKVS_LOGS_PATH = "hdfs:///tmp/tkvs/logs/";
    private Logger log;
    private ArrayList<Object> hdfsLog;

    public HDFSLogger(Class c) {
        if (ALSO_USE_LOG4J)
            log = Logger.getLogger(c);
        hdfsLog = new ArrayList<>();
    }

    public void info(Object m, Class c) {
        if (ALSO_USE_LOG4J)
            log.info(m);
        hdfsLog.add("[" + TimeStamp.getCurrentTime().toDateString() + " INFO " + c.getSimpleName() + "]: " + m);
    }

    public void warn(Object m, Class c) {
        if (ALSO_USE_LOG4J)
            log.warn(m);
        hdfsLog.add("[" + TimeStamp.getCurrentTime().toDateString() + " WARN " + c.getSimpleName() + "]: " + m);
    }

    public void error(Object m, Class c) {
        if (ALSO_USE_LOG4J)
            log.error(m);
        hdfsLog.add("[" + TimeStamp.getCurrentTime().toDateString() + " ERROR " + c.getSimpleName() + "]: " + m);
    }

    public void fatal(Object m, Class c) {
        if (ALSO_USE_LOG4J)
            log.fatal(m);
        hdfsLog.add("[" + TimeStamp.getCurrentTime().toDateString() + " FATAL " + c.getSimpleName() + "]: " + m);
    }

    public void info(Object m, Throwable t, Class c) {
        if (ALSO_USE_LOG4J)
            log.info(m, t);
        hdfsLog.add("[" + TimeStamp.getCurrentTime().toDateString() + " INFO " + c.getSimpleName() + "]: " + m + "\n" + t);
    }

    public void warn(Object m, Throwable t, Class c) {
        if (ALSO_USE_LOG4J)
            log.warn(m, t);
        hdfsLog.add("[" + TimeStamp.getCurrentTime().toDateString() + " WARN " + c.getSimpleName() + "]: " + m + "\n" + t);
    }

    public void error(Object m, Throwable t, Class c) {
        if (ALSO_USE_LOG4J)
            log.error(m, t);
        hdfsLog.add("[" + TimeStamp.getCurrentTime().toDateString() + " ERROR " + c.getSimpleName() + "]: " + m + "\n" + t);
    }

    public void fatal(Object m, Throwable t, Class c) {
        if (ALSO_USE_LOG4J)
            log.fatal(m, t);
        hdfsLog.add("[" + TimeStamp.getCurrentTime().toDateString() + " FATAL " + c.getSimpleName() + "]: " + m + "\n" + t);
    }

    public void writeToHDFS(String contId) {
        if (ALSO_USE_LOG4J)
            log.info("HDFSLogger writing to HDFS...");
        try {
            Path logFile = new Path(TKVS_LOGS_PATH, contId);
            FileSystem fs = logFile.getFileSystem(new YarnConfiguration());
            fs.delete(new Path(TKVS_LOGS_PATH), true); // delete old log dir.
            PrintWriter pr = new PrintWriter(new OutputStreamWriter(fs.create(logFile, true)));
            for (Object s : hdfsLog) {
                pr.println(s.toString());
            }
            pr.close();
            fs.close();
        } catch (Exception e) {
            if (ALSO_USE_LOG4J)
                log.fatal("HDFSLogger could not write logs.", e);
        }
    }
}
