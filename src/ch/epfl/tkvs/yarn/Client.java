package ch.epfl.tkvs.yarn;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.log4j.Logger;

import ch.epfl.tkvs.test.userclient.UserClient;


public class Client {

    private static Logger log = Logger.getLogger(Client.class.getName());
    private YarnConfiguration conf;

    public static void main(String[] args) {
        try {
            log.info("Initializing...");
            new Client().run();
        } catch (Exception ex) {
            log.fatal("Could not run yarn client", ex);
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
        amCLC.setCommands(Collections.singletonList("$JAVA_HOME/bin/java ch.epfl.tkvs.yarn.appmaster.AppMaster"));

        // Set AM jar
        LocalResource jar = Records.newRecord(LocalResource.class);
        Utils.setUpLocalResource(Utils.TKVS_JAR_PATH, jar, conf);
        amCLC.setLocalResources(Collections.singletonMap(Utils.TKVS_JAR_NAME, jar));

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
        log.info("Submitting " + id);
        client.submitApplication(appContext);

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(".last_app_id")));
        writer.write(id.toString());
        writer.close();

        // REPL
        System.out.println("\nClient REPL:");
        ApplicationReport appReport = client.getApplicationReport(id);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED && appState != YarnApplicationState.KILLED
                && appState != YarnApplicationState.FAILED) {

            String input = System.console().readLine("> ");
            switch (input) {
            case ":kill":
                client.killApplication(id);
                log.info("Killing " + id);
                break;
            case ":test":
                System.out.println("Running test client...\n");
                new UserClient().run();
                System.out.println();
                break;
            case "":
                break;
            default:
                System.out.println("Command Not Found!");
            }
            // TODO: more cses ..

            appReport = client.getApplicationReport(id);
            appState = appReport.getYarnApplicationState();
        }
        client.stop();
    }
}
