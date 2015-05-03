package ch.epfl.tkvs.yarn;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import ch.epfl.tkvs.test.userclient.UserClient;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockingUnitTest;
import ch.epfl.tkvs.transactionmanager.versioningunit.VersioningUnitMVCC2PLTest;
import ch.epfl.tkvs.transactionmanager.versioningunit.VersioningUnitMVTOTest;


public class Client {

    private static Logger log = Logger.getLogger(Client.class.getName());

    public static void main(String[] args) {
        try {
            log.info("Initializing...");
            new Client().run();
        } catch (Exception ex) {
            log.fatal("Could not run yarn client", ex);
        }
    }

    private void run() throws Exception {
        YarnConfiguration conf = new YarnConfiguration();

        // Create Yarn Client
        YarnClient client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();

        // Create Application
        YarnClientApplication app = client.createApplication();

        // Create AM Container
        ContainerLaunchContext amCLC = Records.newRecord(ContainerLaunchContext.class);
        amCLC.setCommands(Collections.singletonList("$HADOOP_HOME/bin/hadoop jar TKVS.jar ch.epfl.tkvs.yarn.appmaster.AppMaster" + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

        // Set AM jar
        LocalResource jar = Records.newRecord(LocalResource.class);
        Utils.setUpLocalResource(Utils.TKVS_JAR_PATH, jar, conf);
        amCLC.setLocalResources(Collections.singletonMap(Utils.TKVS_JAR_NAME, jar));

        // Set AM CLASSPATH
        Map<String, String> env = new HashMap<>();
        Utils.setUpEnv(env, conf);
        amCLC.setEnvironment(env);

        // Set AM resources
        Resource res = Records.newRecord(Resource.class);
        res.setMemory(Utils.AM_MEMORY);
        res.setVirtualCores(Utils.AM_CORES);

        if (UserGroupInformation.isSecurityEnabled()) {
            log.info("Setting up security information");
            // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
            Credentials credentials = new Credentials();
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new Exception("Can't get Master Kerberos principal for the RM to use as renewer");
            }

            // For now, only getting tokens for the default file-system.
            FileSystem fs = FileSystem.get(conf);
            final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null) {
                for (Token<?> token : tokens) {
                    log.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amCLC.setTokens(fsTokens);
        }

        // Create ApplicationSubmissionContext
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setKeepContainersAcrossApplicationAttempts(false);
        appContext.setApplicationName("TKVS");
        appContext.setQueue("default");
        appContext.setAMContainerSpec(amCLC);
        appContext.setResource(res);

        // Submit Application
        ApplicationId id = appContext.getApplicationId();
        log.info("Submitting " + id);
        client.submitApplication(appContext);

        // Write the id of the app in a hidden file
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(".last_app_id")));
        writer.write(id.toString());
        writer.close();

        // REPL
        ApplicationReport appReport = client.getApplicationReport(id);
        YarnApplicationState appState = appReport.getYarnApplicationState();

        // Wait until Client knows AM's host:port
        log.info("Waiting for AppMaster.. ");
        while (appReport.getRpcPort() == -1) {
            appReport = client.getApplicationReport(id);
            appState = appReport.getYarnApplicationState();
            if (appState == YarnApplicationState.FINISHED || appState == YarnApplicationState.KILLED || appState == YarnApplicationState.FAILED) {
                log.fatal("AppMaster is dead!");
                System.exit(1);
            }
            Thread.sleep(100);
        }
        Utils.writeAMAddress(Utils.extractIP(appReport.getHost()) + ":" + appReport.getRpcPort());

        Thread.sleep(2000); // Wait a bit until everything is set up.
        System.out.println("\nClient REPL: ");
        while (appState != YarnApplicationState.FINISHED && appState != YarnApplicationState.KILLED && appState != YarnApplicationState.FAILED) {

            String input = System.console().readLine("> ");
            if (input.equals(":exit")) {
                log.info("Stopping gracefully " + id);
                Socket exitSock = new Socket(appReport.getHost(), appReport.getRpcPort());
                PrintWriter out = new PrintWriter(exitSock.getOutputStream(), true);
                out.println(input);
                out.close();
                exitSock.close();
                break;
            }

            // TODO: support more commands with more cases ..
            switch (input) {
            case ":test":
                System.out.println("Running test client...\n");

                log.info("Running example client program...");
                new UserClient().run();
                runTestCases();

                System.out.println();
                break;
            case "":
                break;
            default:
                System.out.println("Command Not Found!");
            }

            appReport = client.getApplicationReport(id);
            appState = appReport.getYarnApplicationState();
        }

        while (appState != YarnApplicationState.FINISHED && appState != YarnApplicationState.KILLED && appState != YarnApplicationState.FAILED) {
            appReport = client.getApplicationReport(id);
            appState = appReport.getYarnApplicationState();
            Thread.sleep(300);
        }
        log.info("Application Stopped with state: " + appState.toString());
        client.stop();
    }

    private static void runTestCases() {
        log.info("Running LockingUnitTest... (might take a while)");
        runTestCase(LockingUnitTest.class);

        log.info("Running VersioningUnitMVCC2PLTest...");
        runTestCase(VersioningUnitMVCC2PLTest.class);

        log.info("Running VersioningUnitMVTOTest...");
        runTestCase(VersioningUnitMVTOTest.class);

        // log.info("Running MVCC2PLTest...");
        // runTestCase(MVCC2PLTest.class);
    }

    private static void runTestCase(Class<?> testCase) {
        Result res = JUnitCore.runClasses(testCase);

        if (res.getFailureCount() == 0) {
            log.info("All tests passed for " + testCase.getSimpleName());
        }

        for (Failure failure : res.getFailures()) {
            log.error(failure.toString());
        }
    }
}
