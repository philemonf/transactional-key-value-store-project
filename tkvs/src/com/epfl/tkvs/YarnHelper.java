package com.epfl.tkvs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class YarnHelper {
	public static String buildCP(YarnConfiguration conf) {
		StringBuilder cp = new StringBuilder(
				ApplicationConstants.Environment.APP_CLASSPATH.$());

		String[] yarnCP = conf.getStrings(conf.YARN_APPLICATION_CLASSPATH,
				conf.DEFAULT_YARN_APPLICATION_CLASSPATH);
		for (String yarnClass : yarnCP) {
			cp.append(yarnClass).append(":");
		}

		return cp.toString();
	}

	public static LocalResource getLocalResourceForJar(Path jarPath,
			Configuration conf) throws IOException {
		FileStatus fStatus = jarPath.getFileSystem(conf).getFileStatus(jarPath);
		return LocalResource.newInstance(
				ConverterUtils.getYarnUrlFromPath(jarPath),
				LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
				fStatus.getLen(), fStatus.getModificationTime());
	}
}
