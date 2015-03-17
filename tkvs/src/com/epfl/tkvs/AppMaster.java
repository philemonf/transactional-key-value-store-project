package com.epfl.tkvs;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


public class AppMaster implements AMRMClientAsync.CallbackHandler{

	private YarnConfiguration conf;
	
	public static void main(String[] args) throws Exception {
		new AppMaster().run();
	}
	
	public void run() throws Exception {
		conf = new YarnConfiguration();
		
		AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
		rmClient.init(conf);
		rmClient.start();
		
		rmClient.registerApplicationMaster(NetUtils.getHostname(), 0, "");
		
		Thread.sleep(1000);
		System.out.println("Test log");
		
		// TODO change container request so that it creates one in each node
		Resource capability = Resource.newInstance(256, 1);
		Priority priority = Priority.newInstance(0);
		String[] hosts = null;
		String[] racks = null;
		ContainerRequest containerRequest = new ContainerRequest(capability, hosts, racks, priority);
		rmClient.addContainerRequest(containerRequest);
		
		rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
	}

	@Override
	public float getProgress() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
	}
	
	private ContainerLaunchContext getContainerLaunchContext() {
		String jarName = "TransactionManagerDeamon.jar";
		
		Vector<String> cmds = new Vector<String>();
		cmds.add("$JAVA_HOME/bin/java");
		cmds.add("com.epfl.tkvs.TransactionManagerDeamon");
		cmds.add("-cp " + jarName);
		
		
		Path jarPath = new Path("/tkvs/" + jarName);
		try {
			Map<String, LocalResource> localResources = Collections.singletonMap("TransactionManagerDeamon.jar",
					YarnHelper.getLocalResourceForJar(jarPath, conf));
			
			Map<String, String> env = Collections.singletonMap(Environment.CLASSPATH.name(), YarnHelper.buildCP(conf));
			return ContainerLaunchContext.newInstance(localResources, env, cmds, null, null, null);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	

	@Override
	public void onContainersCompleted(List<ContainerStatus> statusOfContainers) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onError(Throwable e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onNodesUpdated(List<NodeReport> nodeReports) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onShutdownRequest() {
		// TODO Auto-generated method stub
		
	}
}
