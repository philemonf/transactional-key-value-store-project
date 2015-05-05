package ch.epfl.tkvs.test.userclient;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Random;

import ch.epfl.tkvs.transactionmanager.AbortException;
import ch.epfl.tkvs.user.Key;
import ch.epfl.tkvs.user.Transaction;


public class Benchmark {

    private enum ActionType {
        WRITE, READ
    }

    private class Action {

        private MyKey key;
        private ActionType type;

        /**
         * 
         * @param type of the Action (WRITE or READ)
         * @param key id
         */
        public Action(ActionType type, MyKey key) {

            this.type = type;
            this.key = key;
        }

    }

    private enum BenchmarkStatus {
        BEGIN, READ, WRITE, COMMIT
    }

    private final String BENCHMARK_RESULTS_FILE = "results.bm";
    private PrintWriter pw;

    // Keys the users will access
    private MyKey keys[];
    // Users threads which will execute actions
    private User users[];
    // Associate a list of action to a user
    private Action userActions[][];

    // Maximal number of action for a user
    private int maxNbActions;
    // Ratio of read compared to one write
    private int ratio;
    // Repeat the same number of action multiple time
    private int repetitions;

    // Time for one execution of the benchmark
    private double runningTime;

    /**
     * 
     * @param repetition: Number of time the benchmark will be repeated with the same parameters
     * @param nbKeys: Number of keys that will be accessed by the users
     * @param nbUsers: Number of users for the benchmark
     * @param maxNbActions: Max number of actions that will be done by the user
     * @param ratio: Number of read for one write
     */
    public Benchmark(int nbKeys, int nbUsers, int maxNbActions, int ratio, int repetitions) {

        this.keys = new MyKey[nbKeys];
        for (int i = 0; i < nbKeys; i++) {
            keys[i] = new MyKey("Key" + i);
        }

        this.users = new User[nbUsers];
        this.userActions = new Action[nbUsers][];

        this.maxNbActions = maxNbActions;
        this.ratio = ratio;
        this.repetitions = repetitions;
    }

    public void run() throws FileNotFoundException {

        System.out.println("Benchmarking start");
        // Append the results to the same file
        pw = new PrintWriter(new FileOutputStream(BENCHMARK_RESULTS_FILE, true));

        printBenchmarkParameters();
        initializeUsersActions();

        for (int i = 0; i < repetitions; i++) {
            initializeKeys();
            startBenchmarking();
            extractResults(pw);
        }

        pw.close();
        System.out.println("Benchmarking end");
    }

    /**
     * Print the different parameters entered
     */
    private void printBenchmarkParameters() {

        System.out.println("\tParameters:");
        System.out.println("\t\tNumber of transactions: " + users.length);
        System.out.println("\t\tMaximum number of requests: " + maxNbActions);
        System.out.println("\t\tNumber of keys: " + keys.length);
        System.out.println("\t\tread:write ratio: " + ratio + ":1");
    }

    /**
     * Create a transaction to write into the Key Value store in order to initialize the keys used for the benchmark
     * 
     */
    private void initializeKeys() {

        boolean isDone = false;
        boolean aborted = false;

        while (!isDone) {
            isDone = true;
            aborted = false;
            Transaction<Key> init = null;

            try {
                init = new Transaction<Key>(new MyKey("init"));
            } catch (AbortException e) {
                // TODO Handle this case
                e.printStackTrace();
            }

            for (int i = 0; i < keys.length; i++) {
                try {
                    init.write(keys[i], "init" + i);
                } catch (AbortException e) {
                    aborted = true;
                }
            }

            if (aborted) {
                continue;
            }

            try {
                init.commit();
            } catch (AbortException e) {
                isDone = false;
            }
        }
    }

    /**
     * Initialize the list of action the users will execute in the benchmark
     *
     */
    private void initializeUsersActions() {

        for (int i = 0; i < userActions.length; i++) {
            // Generate a random number of Actions
            Random r = new Random();
            int nbActions = r.nextInt(maxNbActions) + 1;
            userActions[i] = new Action[nbActions];

            for (int j = 0; j < userActions[i].length; j++) {
                int keyIndex = 0;
                keyIndex = r.nextInt(keys.length);

                // Determine if we want to read or write a key
                int write = r.nextInt(ratio);
                if (write != 0) {
                    userActions[i][j] = new Action(ActionType.READ, keys[keyIndex]);
                } else {
                    userActions[i][j] = new Action(ActionType.WRITE, keys[keyIndex]);
                }
            }
        }
    }

    /**
     * Initialize the Users threads by specifying their actions Start the threads to run concurrently Wait for all the
     * threads to stop
     */
    private void startBenchmarking() {
        // Init the threads
        for (int i = 0; i < users.length; i++) {
            users[i] = new User(i + 1, userActions[i]);
        }

        runningTime = System.currentTimeMillis();

        // Launch the threads
        for (int i = 0; i < users.length; i++) {
            users[i].start();
        }

        // Wait for the threads to end
        for (int i = 0; i < users.length; i++) {
            try {
                users[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        runningTime = System.currentTimeMillis() - runningTime;
    }

    /**
     * Extract all the information needed from the User threads
     * @param pw
     */
    private void extractResults(PrintWriter pw) {

        int nbReadTotal = 0;
        int nbWriteTotal = 0;
        int nbBeginAbortsTotal = 0;
        int nbReadAbortsTotal = 0;
        int nbWriteAbortsTotal = 0;
        int nbCommitAbortsTotal = 0;
        int nbCommitTotal = 0;
        double sumLatency = 0;

        System.out.println("Benchmark results on " + keys.length + " keys");

        for (int i = 0; i < users.length; i++) {

            int nbReadActions = 0;
            for (int j = 0; j < users[i].actions.length; j++) {
                if (users[i].actions[j].type == ActionType.READ) {
                    nbReadActions++;
                }
            }
            int nbWriteActions = users[i].actions.length - nbReadActions;

            System.out.println("T" + users[i].userID + ":\t#readActions = " + nbReadActions + "\t#writeActions = " + nbWriteActions + "\t#aborts = " + (users[i].nbReadAborts + users[i].nbWriteAborts));

            sumLatency += users[i].latency;
            nbReadTotal += users[i].nbRead;
            nbWriteTotal += users[i].nbWrite;
            nbCommitTotal += users[i].nbCommit;
            nbBeginAbortsTotal += users[i].nbBeginAborts;
            nbWriteAbortsTotal += users[i].nbWriteAborts;
            nbReadAbortsTotal += users[i].nbReadAborts;
            nbCommitAbortsTotal += users[i].nbCommitAborts;
        }

        double latency = sumLatency / users.length;
        double throughput = runningTime / users.length;
        int nbAbortTotal = nbBeginAbortsTotal + nbReadAbortsTotal + nbWriteAbortsTotal + nbCommitAbortsTotal;
        System.out.println("Results:");
        System.out.println("\tNumber of Reads: " + nbReadTotal);
        System.out.println("\tNumber of Writes: " + nbWriteTotal);
        System.out.println("\tNumber of Aborts on Begin: " + nbBeginAbortsTotal);
        System.out.println("\tNumber of Aborts on Read: " + nbReadAbortsTotal);
        System.out.println("\tNumber of Aborts on Write: " + nbWriteAbortsTotal);
        System.out.println("\tNumber of Aborts on Commit: " + nbCommitAbortsTotal);
        System.out.println("\tTotal Commit: " + nbCommitTotal);
        System.out.println("\tTotal Aborts: " + nbAbortTotal);
        System.out.println("\tTotal Latency: " + latency + " ms/transaction");
        System.out.println("\tThroughput: " + throughput + " transaction/ms");

        pw.format("%d %d %d %d %d %d %d %d %d %f %f\n", users.length, keys.length, ratio, nbReadTotal, nbReadAbortsTotal, nbWriteTotal, nbWriteAbortsTotal, nbCommitTotal, nbAbortTotal, latency, throughput);
        pw.flush();
    }

    private class User extends Thread {

        // ID of the user
        private int userID;
        // Number of read done by the user
        private int nbRead;
        // Number of write done by the user
        private int nbWrite;
        // Number of aborts generated by a begin
        private int nbBeginAborts;
        // Number of aborts generated by a read
        private int nbReadAborts;
        // Number of aborts generated by a write
        private int nbWriteAborts;
        // Number of aborts generated by a commit
        private int nbCommitAborts;
        // Number of commits
        private int nbCommit;
        // Time for the user to complete its transaction
        private double latency;
        // Contains the actions of the users in order
        private Action actions[];

        /**
         * 
         * @param userID
         * @param actions
         */
        public User(int userID, Action actions[]) {

            this.userID = userID;
            this.actions = actions;

            this.nbRead = 0;
            this.nbWrite = 0;
            this.nbBeginAborts = 0;
            this.nbReadAborts = 0;
            this.nbWriteAborts = 0;
            this.nbCommitAborts = 0;
            this.nbCommit = 0;
            this.latency = 0;

        }

        @Override
        public void run() {

            BenchmarkStatus benchmarkStatus = BenchmarkStatus.BEGIN;
            boolean isDone = false;
            latency = System.currentTimeMillis();

            while (!isDone) {
                MyKey key = actions[0].key;
                Transaction<MyKey> t = null;

                try {

                    benchmarkStatus = BenchmarkStatus.BEGIN;
                    t = new Transaction<MyKey>(key);

                    for (int i = 0; i < actions.length; i++) {
                        key = actions[i].key;
                        switch (actions[i].type) {
                        case WRITE:
                            benchmarkStatus = BenchmarkStatus.WRITE;
                            nbWrite++;
                            t.write(key, "UserID:" + userID + " Action:" + i);
                            break;
                        case READ:
                            benchmarkStatus = BenchmarkStatus.READ;
                            nbRead++;
                            t.read(key);
                            break;
                        }
                    }

                    benchmarkStatus = BenchmarkStatus.COMMIT;
                    t.commit();
                    nbCommit = nbCommit + 1;
                    isDone = true;

                } catch (AbortException e) {
                    switch (benchmarkStatus) {
                    case BEGIN:
                        nbBeginAborts++;
                        break;
                    case READ:
                        nbReadAborts++;
                        break;
                    case WRITE:
                        nbWriteAborts++;
                        break;
                    case COMMIT:
                        nbCommitAborts++;
                    }
                }

            }
            latency = System.currentTimeMillis() - latency;
        }
    }
}
