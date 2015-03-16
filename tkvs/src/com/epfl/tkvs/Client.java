package com.epfl.tkvs;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class Client {

	public static void main(String[] args) throws Exception {
		YarnClient client = YarnClient.createYarnClient();
		client.init(new YarnConfiguration());
		client.start();

		YarnClientApplication app = client.createApplication();

		ContainerLaunchContext containerSpec = Records
				.newRecord(ContainerLaunchContext.class);
		List<String> commands = new LinkedList<String>();
		commands.add("$JAVA_HOME/bin/java");
		commands.add("-Xmx256M");
		commands.add("com.epfl.tkvs.AppMaster");
		containerSpec.setCommands(commands);

		Path jarPath = new Path(args[0]);
		FileSystem fs = FileSystem.get(client.getConfig());
		FileStatus jarStatus = fs.getFileStatus(jarPath);
		LocalResource amJarRsrc = LocalResource.newInstance(
				ConverterUtils.getYarnUrlFromPath(jarPath),
				LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
				jarStatus.getLen(), jarStatus.getModificationTime());
		
		System.out.println("AppMaster Path: " + ConverterUtils.getYarnUrlFromPath(jarPath));
		
		Map<String, LocalResource> localResource = Collections.singletonMap(jarPath.getName(), amJarRsrc);
		containerSpec.setLocalResources(localResource);

		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(256);
		capability.setVirtualCores(1);

		ApplicationSubmissionContext appContext = app
				.getApplicationSubmissionContext();
		appContext.setQueue("default");
		appContext.setAMContainerSpec(containerSpec);
		appContext.setResource(capability);
		appContext.setApplicationName("Transactional Key-Value Store");

		client.submitApplication(appContext);
	}
}
