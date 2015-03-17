package ch.epfl.tkvs;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class Utils {
	public static final Path TKVS_JAR = new Path("/apps/tkvs/TKVS.jar");

	public static void setUpEnv(Map<String, String> env, YarnConfiguration conf) {
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
										YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			Apps.addToEnvironment(env, Environment.CLASSPATH.name(), c.trim(), File.separator);
		}
		Apps.addToEnvironment(env, Environment.CLASSPATH.name(), Environment.PWD.$(), File.separator + "*");
	}

	public static void setUpLocalResource(Path resPath, LocalResource res, YarnConfiguration conf) throws IOException {
		FileStatus status = FileSystem.get(conf).getFileStatus(resPath);
		res.setResource(ConverterUtils.getYarnUrlFromPath(resPath));
		res.setSize(status.getLen());
		res.setTimestamp(status.getModificationTime());
		res.setType(LocalResourceType.FILE);
		res.setVisibility(LocalResourceVisibility.PUBLIC);
	}
}
