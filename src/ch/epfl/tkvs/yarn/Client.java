package ch.epfl.tkvs.yarn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
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
import org.apache.log4j.Logger;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import ch.epfl.tkvs.test.userclient.Benchmark;
import ch.epfl.tkvs.test.userclient.MV2PLSystemTest;
import ch.epfl.tkvs.test.userclient.MVTOSystemTest;
import ch.epfl.tkvs.test.userclient.S2PLSystemTest;
import ch.epfl.tkvs.transactionmanager.algorithms.MVCC2PLTest;
import ch.epfl.tkvs.transactionmanager.algorithms.Simple2PLTest;
import ch.epfl.tkvs.transactionmanager.lockingunit.DeadlockGraphTest;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockingUnitTest;
import ch.epfl.tkvs.transactionmanager.versioningunit.VersioningUnitMVCC2PLTest;
import ch.epfl.tkvs.transactionmanager.versioningunit.VersioningUnitMVTOTest;
import ch.epfl.tkvs.user.UserTransaction;


/**
 * The YARN Client is responsible for launching the Application Master (AM). Prepares the AM's container and launches
 * the process in it. As soon as the AM is ready, the REPL shows up to the user (read-eval-print loop). The REPL
 * supports testing, benchmarking, exiting gracefully, and is extensible.
 * @see ch.epfl.tkvs.yarn.appmaster.AppMaster
 */
public class Client {

    private final static Logger log = Logger.getLogger(Client.class.getName());
    private static String algoConfig;
    public static void main(String[] args) {
        Utils.initLogLevel();
        try {
            log.info("Initializing...");
            
            // Read the concurrency control algorithm that one want to use
            algoConfig = Utils.readAlgorithmConfig();
            
            new Client().run();
        } catch (Exception ex) {
            log.fatal("Could not run yarn client", ex);
        }
    }

    private void run() throws Exception {
        YarnConfiguration conf = new YarnConfiguration();

        // Create Yarn Client.
        YarnClient client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();

        // Create Application.
        ApplicationSubmissionContext appContext = createAppMaster(client.createApplication(), conf);

        // Submit Application.
        ApplicationId id = appContext.getApplicationId();
        log.info("Submitting " + id);
        client.submitApplication(appContext);

        // Write the id of the app in a hidden file.
        Utils.writeAppId(id);

        // REPL
        ApplicationReport appReport = client.getApplicationReport(id);
        YarnApplicationState appState = appReport.getYarnApplicationState();

        // Wait until Client knows AM's ip:port.
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
        String amIp = Utils.extractIP(appReport.getHost());
        int amPort = appReport.getRpcPort();
        Utils.writeAMAddress(amIp + ":" + amPort);

        log.info("AppMaster Address: " + amIp + ":" + amPort);
        // Ping the AppMaster until it is ready
        log.info("Start pinging the AppMaster until it is ready.");
        while (!pingAppMaster(amIp, amPort)) {
            Thread.sleep(3000);
            System.out.print(".");
        }

        ArrayList<String> hist = Utils.loadREPLHist();
        System.out.println("\nClient REPL: ");
        Scanner scanner = new Scanner(System.in);
        while (appState != YarnApplicationState.FINISHED && appState != YarnApplicationState.KILLED && appState != YarnApplicationState.FAILED) {
            String input = ":exit"; // Default REPL command is :exit.
            Thread.sleep(500); // Useful for batch commands.

            System.out.print("> ");
            if (scanner.hasNextLine()) {
                input = scanner.nextLine();
            }

            if (input.equals(":exit")) {
                log.info("Stopping gracefully " + id);
                Socket exitSock = new Socket(amIp, amPort);
                PrintWriter out = new PrintWriter(exitSock.getOutputStream(), true);
                out.println(input);
                out.close();
                exitSock.close();
                break;
            }

            // TODO: support more commands with more cases ..
            switch (input.split(" ")[0]) {
            case ":hist":
                for (int i = 0; i < hist.size(); ++i) {
                    System.out.println(i + ": " + hist.get(i));
                }
                break;
            case ":test":
                hist.add(input);

                System.out.println("Running test client...\n");
                
                // Run the appropriate system test given the selected algorithm
                if (algoConfig.equals("simple_2pl")) {
                	runTestCase(S2PLSystemTest.class);
                } else if (algoConfig.equals("mvcc2pl")) {
                	runTestCase(MV2PLSystemTest.class);
                } else {
                	runTestCase(MVTOSystemTest.class);
                }
                
                UserTransaction.log.writeToHDFS("C");

                System.out.println();
                break;
            case ":testall":
                System.out.println("Running unit tests...\n");
                runTestCases();
                break;
            case ":benchmark":
                hist.add(input);
                Pattern pattern = Pattern.compile(":benchmark t (\\d+) r (\\d+) k (\\d+) ratio (\\d+)(?: )?(\\d+)?");
                Matcher matcher = pattern.matcher(input);
                if (!matcher.matches()) {
                    System.out.println("Usage ``:benchmark t <#transactions> r <#requestsPerTransaction> k <#keys> ratio <readWriteRatio> <#repetitions>");
                    System.out.println("Run t transactions, doing each at most r random actions, using a set of k keys with ratio:1 read:write ratio");
                    break;
                }

                int nbUsers = Math.max(1, Integer.parseInt(matcher.group(1)));
                int maxNbActions = Math.max(1, Integer.parseInt(matcher.group(2)));
                int nbKeys = Math.max(1, Integer.parseInt(matcher.group(3)));
                int ratio = Math.max(2, Integer.parseInt(matcher.group(4)));

                int repetition = 1;
                if (matcher.group(5) != null) {
                    repetition = Integer.parseInt(matcher.group(5));
                }

                new Benchmark(nbKeys, nbUsers, maxNbActions, ratio, repetition).run();
                break;

            case ":help":
            	System.out.println(":benchmark - run some benchmark");
            	System.out.println(":testall - run unit tests");
            	System.out.println(":test - run system test");
            	System.out.println(":exit - exit the system properly");
            	break;
                
            case "":
                break;
            default:
                hist.add(input);
                System.out.println("Command Not Found!");
            }

            appReport = client.getApplicationReport(id);
            appState = appReport.getYarnApplicationState();
        }

        // Wait for the AM to finish.
        scanner.close();
        Utils.writeREPLHist(hist);
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

        log.info("Running MVCC2PLTest...");
        runTestCase(MVCC2PLTest.class);
        
        log.info("Running Simple2PLTest...");
        runTestCase(Simple2PLTest.class);
        
        log.info("Running DeadlockGraphTest...");
        runTestCase(DeadlockGraphTest.class);
    }

    private static void runTestCase(Class<?> testCase) {
        Result res = JUnitCore.runClasses(testCase);

        if (res.getFailureCount() == 0) {
            log.info(res.getRunCount() + " tests passed for " + testCase.getSimpleName());
        }

        for (Failure failure : res.getFailures()) {
            log.error(failure.toString());
        }
    }

    private boolean pingAppMaster(String ip, int port) throws IOException {
        final Socket sock = new Socket(ip, port);
        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));

        try {
            // Ping the AppMaster
            out.println(":ping");
            out.flush();

            // Wait for a response
            String response = in.readLine();

            if (response != null && response.equals("ok")) {
                return true;
            }

            return false;

        } finally {
            out.close();
            in.close();
            sock.close();
        }
    }

    private ApplicationSubmissionContext createAppMaster(YarnClientApplication app, YarnConfiguration conf) throws Exception {
        ContainerLaunchContext amCLC = Records.newRecord(ContainerLaunchContext.class);

        // TODO: Fix the logging method from ApplicationConstants.LOG_DIR_EXPANSION_VAR.
        amCLC.setCommands(Collections.singletonList("$HADOOP_HOME/bin/hadoop jar TKVS.jar ch.epfl.tkvs.yarn.appmaster.AppMaster " + algoConfig + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

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
        return appContext;
    }
}
