package ch.epfl.tkvs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

public class Client {

	private YarnConfiguration conf;

	public static void main(String[] args){
		System.out.println("TKVS Client: Initializing");
		try {
			new Client().run();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void run() throws Exception {
		conf = new YarnConfiguration();

		// Create Yarn Client
		YarnClient client = YarnClient.createYarnClient();
		client.init(conf);
		client.start();

		// Create Application
		YarnClientApplication app = client.createApplication();

		// Create AM Container
		ContainerLaunchContext amCLC = Records.newRecord(ContainerLaunchContext.class);
		amCLC.setCommands(Collections.singletonList("$JAVA_HOME/bin/java"
				+ " -Xmx256M"
				+ " ch.epfl.tkvs.AppMaster"
				+ " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
				+ " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

		// Set AM jar
		LocalResource jar = Records.newRecord(LocalResource.class);
		Utils.setUpLocalResource(Utils.TKVS_JAR, jar, conf);
		amCLC.setLocalResources(Collections.singletonMap("TKVS.jar", jar));

		// Set AM CLASSPATH
		Map<String, String> env = new HashMap<String, String>();
		Utils.setUpEnv(env, conf);
		amCLC.setEnvironment(env);

		// Set AM resources
		Resource res = Records.newRecord(Resource.class);
		res.setMemory(256);
		res.setVirtualCores(1);

		// Create ApplicationSubmissionContext
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		appContext.setApplicationName("TKVS");
		appContext.setQueue("default");
		appContext.setAMContainerSpec(amCLC);
		appContext.setResource(res);

		// Submit Application
		ApplicationId id = appContext.getApplicationId();
		System.out.println("TKVS Client: Submitting " + id);
		client.submitApplication(appContext);

		ApplicationReport appReport = client.getApplicationReport(id);
		YarnApplicationState appState = appReport.getYarnApplicationState();
		while (appState != YarnApplicationState.FINISHED
				&& appState != YarnApplicationState.KILLED
				&& appState != YarnApplicationState.FAILED) {
			Thread.sleep(1000);
			appReport = client.getApplicationReport(id);
			appState = appReport.getYarnApplicationState();
		}

		System.out.println("TKVS Client: Finished " + id + " with state " + appState);
	}

}
