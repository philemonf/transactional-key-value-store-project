import static java.util.Arrays.asList;
import static org.apache.hadoop.yarn.api.ApplicationConstants.LOG_DIR_EXPANSION_VAR;
import static org.apache.hadoop.yarn.client.api.YarnClient.createYarnClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class Client {

	private YarnConfiguration yarnConfiguration = null;
	private YarnClient yarnClient = null;
	private YarnClientApplication application = null;

	public static void main(String[] args) throws Exception {
		new Client().run(args);
	}

	public void run(String[] args) throws YarnException, IOException {

		String amJarPath = args[0];

		initializeYarnClient();

		// Create the YARN application
		application = yarnClient.createApplication();

		// Submit the application
		submitApplication(new Path(amJarPath));
	}

	private void initializeYarnClient() {
		yarnClient = createYarnClient();

		yarnConfiguration = new YarnConfiguration();
		yarnClient.init(yarnConfiguration);

		yarnClient.start();
	}

	private void submitApplication(Path amJarPath) throws IOException, YarnException {

		LinkedList<String> hosts = new LinkedList<String>();
		for (NodeId nodeId : yarnClient.getNodeToLabels().keySet()) {
			hosts.add(nodeId.getHost());
		}
		
		System.out.println(hosts);
		
		  // Set the necessary command to execute the application master
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		vargs.add("-Xmx" + 256 + "m");
		vargs.add("ApplicationMaster");
		vargs.add(hosts.toString());
		vargs.add("--container_memory 1024");
		vargs.add("--container_cores 1");
		vargs.add("--num_containers 3");
		vargs.add("1>" + LOG_DIR_EXPANSION_VAR + "/TransactionMaster.stdout");
		vargs.add("2>" + LOG_DIR_EXPANSION_VAR + "/TransactionMaster.stderr");  

		
		// Get final commmand
		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
		  command.append(str).append(" ");
		}

		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		Map<String, String> env = new HashMap<String, String>();
		  
		ContainerLaunchContext containerSpec = ContainerLaunchContext.newInstance(localResources, env, asList(command.toString()), null, null, null);
		Resource capability = Resource.newInstance(256, 1);
		  
		ApplicationSubmissionContext appContext = application.getApplicationSubmissionContext();
		appContext.setQueue("trans-kv-queue");
		appContext.setAMContainerSpec(containerSpec);
		appContext.setResource(capability);
		appContext.setApplicationName("Transactional Key-Value Store");
		  
		yarnClient.submitApplication(appContext);
	}
}
