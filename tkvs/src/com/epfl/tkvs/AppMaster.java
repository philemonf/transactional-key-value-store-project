package com.epfl.tkvs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
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
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;


public class AppMaster implements AMRMClientAsync.CallbackHandler{

	private YarnConfiguration conf;
	private NMClient nmClient;
	private int containerCount = 3;
	
	public static void main(String[] args) throws Exception {
		new AppMaster().run();
	}
	
	public void run() throws Exception {
		System.out.println("TM APP MASTER: running");
		conf = new YarnConfiguration();
		
		AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
		rmClient.init(conf);
		rmClient.start();
		
		nmClient = NMClient.createNMClient();
		nmClient.init(conf);
		nmClient.start();
		
		rmClient.registerApplicationMaster("", 0, "");
		
		Thread.sleep(1000);
		System.out.println("TM APP MASTER: Alive!");

		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);

		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(128);
		capability.setVirtualCores(1);

		for (int i = 0; i < containerCount; ++i) {
			ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
			System.out.println("TM APP MASTER: Requesting Container " + i);
			rmClient.addContainerRequest(containerAsk);
		}
		while (!containersFinished()) {
			Thread.sleep(100);
		}

		rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
	}
	
	private boolean containersFinished() {
		return containerCount == 0;
	}

	@Override
	public float getProgress() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
		for (Container container : containers) {
			try {
				System.out.println("TM APP MASTER: Launching container " + container.getId());
				nmClient.startContainer(container, getContainerLaunchContext());

			} catch (Exception ex) {
				System.err.println("TM APP MASTER: Error launching container " + container.getId());
			}
		}
	}
	
	private ContainerLaunchContext getContainerLaunchContext() {
		String jarName = "TransactionManagerDeamon.jar";
		
		Vector<String> cmds = new Vector<String>();
		cmds.add("$JAVA_HOME/bin/java");
		cmds.add("-Xmx256M");
		cmds.add(TransactionManagerDeamon.class.getName());
		cmds.add("-cp " + jarName);
		cmds.add(" 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        cmds.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        
        String commands = "";
        for (String cmd : cmds) {
        	commands += cmd + " ";
        }
		
		Path jarPath = new Path("/tkvs/" + jarName);
		try {
			Map<String, LocalResource> localResources = Collections.singletonMap("TransactionManagerDeamon.jar",
					YarnHelper.getLocalResourceForJar(jarPath, conf));
			
			Map<String, String> env = Collections.singletonMap(Environment.CLASSPATH.name(), YarnHelper.buildCP(conf));
			return ContainerLaunchContext.newInstance(localResources, env, Arrays.asList(commands), null, null, null);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	

	@Override
	public void onContainersCompleted(List<ContainerStatus> statusOfContainers) {
		for (ContainerStatus status : statusOfContainers) {
			System.out.println("TM APP MASTER: Completed container " + status.getContainerId());

			synchronized (this) {
				containerCount--;
			}
		}
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
