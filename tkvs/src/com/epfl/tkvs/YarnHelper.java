package com.epfl.tkvs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class YarnHelper {
	public static String buildCP(YarnConfiguration conf) {
		StringBuilder cp = new StringBuilder(
				ApplicationConstants.Environment.APP_CLASSPATH.$());

		String[] yarnCP = conf.getStrings(
				YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
		for (String yarnClass : yarnCP) {
			cp.append(yarnClass).append(":");
		}

		return cp.toString();
	}

	public static LocalResource getLocalResourceForJar(Path jarPath,
			Configuration conf) throws IOException {
		FileStatus fStatus = jarPath.getFileSystem(conf).getFileStatus(jarPath);

		URL packageUrl = ConverterUtils.getYarnUrlFromPath(FileContext
				.getFileContext().makeQualified(jarPath));

		LocalResource packageResource = Records.newRecord(LocalResource.class);
		packageResource.setResource(packageUrl);
		packageResource.setSize(fStatus.getLen());
		packageResource.setTimestamp(fStatus.getModificationTime());
		packageResource.setType(LocalResourceType.ARCHIVE);
		packageResource.setVisibility(LocalResourceVisibility.APPLICATION);
		
		return packageResource;
	}
}
