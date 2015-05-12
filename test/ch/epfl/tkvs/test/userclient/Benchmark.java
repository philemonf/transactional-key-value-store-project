package ch.epfl.tkvs.test.userclient;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.user.Key;
import ch.epfl.tkvs.user.UserTransaction;


public class Benchmark {

    private static Logger log = Logger.getLogger(Benchmark.class.getName());

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
    // Percentage of keys in a transaction which are on the same node of the key used to create the transaction
    private int localityPercentage;
    // Indicate if the locality is used for the benchmark
    private boolean locality;
    // Associate a localityHash to a list of keys with that locality
    private HashMap<Integer, ArrayList<MyKey>> localityKeys;

    // Time for one execution of the benchmark
    private double runningTime;

    /**
     * @param repetition: Number of time the benchmark will be repeated with the same parameters
     * @param nbKeys: Number of keys that will be accessed by the users
     * @param nbUsers: Number of users for the benchmark
     * @param maxNbActions: Max number of actions that will be done by the user
     * @param ratio: Number of read for one write
     * @param localityPercentage: Percentage of keys in a transaction which are on the same node of the key used to
     * create the transaction
     */
    public Benchmark(int nbKeys, int nbUsers, int maxNbActions, int ratio, int repetitions, int localityPercentage) {

        this.localityKeys = new HashMap<Integer, ArrayList<MyKey>>();

        this.keys = new MyKey[nbKeys];
        for (int i = 0; i < nbKeys; i++) {
            keys[i] = new MyKey("Key" + i, new Random().nextInt(300)); // TODO: think about the key locality hashes

            if (localityKeys.get(keys[i].getLocalityHash()) == null) {
                localityKeys.put(keys[i].getLocalityHash(), new ArrayList<MyKey>());
            }

            ArrayList<MyKey> listKeys = localityKeys.get(keys[i].localityHash);
            listKeys.add(keys[i]);

            localityKeys.put(keys[i].getLocalityHash(), listKeys);
        }

        this.users = new User[nbUsers];
        this.userActions = new Action[nbUsers][];

        this.maxNbActions = maxNbActions;
        this.ratio = ratio;
        this.repetitions = repetitions;

        if (0 <= localityPercentage && localityPercentage <= 100) {
            this.localityPercentage = localityPercentage;
            locality = true;
        } else {
            this.localityPercentage = 0;
            locality = false;
        }

    }

    public void run() throws FileNotFoundException {

        System.out.println("Benchmarking start");
        // Append the results to the same file
        printBenchmarkParameters();
        initializeUsersActions();

        for (int i = 0; i < repetitions; i++) {
            try {
                initializeKeys();
            } catch (AbortException e) {
                log.error("1st Transaction could not write all keys!");
                return;
            }
            startBenchmarking();
            extractResults();
        }

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
     * @throws AbortException
     * 
     */
    private void initializeKeys() throws AbortException {

        boolean isDone = false;

        while (!isDone) {
            isDone = true;
            UserTransaction<Key> init = null;
            init = new UserTransaction<Key>();
            init.begin(keys[new Random().nextInt(keys.length)]); // TODO: choose a better

            for (int i = 0; i < keys.length; i++) {
                init.write(keys[i], "init" + i);
            }

            init.commit();
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

                // Determine if we want to read or write a key
                int write = r.nextInt(ratio);
                if (write != 0) {
                    if (j != 0 && locality) {
                        int randomLocality = r.nextInt(100);
                        MyKey initialKey = userActions[i][0].key;
                        if (randomLocality <= localityPercentage) {
                            // Get all the keys with the same locality hash of the first key
                            ArrayList<MyKey> tmpKeys = localityKeys.get(initialKey.localityHash);
                            keyIndex = r.nextInt(tmpKeys.size());

                            userActions[i][j] = new Action(ActionType.READ, tmpKeys.get(keyIndex));
                        } else {
                            // Get all the localityHash entries
                            ArrayList<Integer> tmpLocalities = new ArrayList<Integer>(localityKeys.keySet());
                            // Remove the initial one
                            int removedLocalityHash = tmpLocalities.remove(tmpLocalities.indexOf(initialKey.getLocalityHash()));

                            ArrayList<MyKey> tmpKeys = localityKeys.get(removedLocalityHash);
                            // From the localityHash left, pick a random one
                            if (tmpLocalities.size() > 0) {

                                int randomLocalityHashIndex = r.nextInt(tmpLocalities.size());
                                // Key all the keys associated to it
                                tmpKeys = localityKeys.get(tmpLocalities.get(randomLocalityHashIndex));
                            }
                            // Pick one key
                            keyIndex = r.nextInt(tmpKeys.size());
                            userActions[i][j] = new Action(ActionType.READ, tmpKeys.get(keyIndex));
                        }
                    } else {
                        keyIndex = r.nextInt(keys.length);
                        userActions[i][j] = new Action(ActionType.READ, keys[keyIndex]);
                    }
                } else {
                    if (j != 0 && locality) {
                        int randomLocality = r.nextInt(100);
                        MyKey initialKey = userActions[i][0].key;
                        // The user will write into a key of the same locality of its first key
                        if (randomLocality <= localityPercentage) {
                            ArrayList<MyKey> tmpKeys = localityKeys.get(initialKey.localityHash);
                            keyIndex = r.nextInt(tmpKeys.size());
                            userActions[i][j] = new Action(ActionType.WRITE, tmpKeys.get(keyIndex));
                        } else {
                            // Get all the localityHash entries
                            ArrayList<Integer> tmpLocalities = new ArrayList<Integer>(localityKeys.keySet());
                            // Remove the initial one
                            int removedLocalityHash = tmpLocalities.remove(tmpLocalities.indexOf(initialKey.getLocalityHash()));

                            ArrayList<MyKey> tmpKeys = localityKeys.get(removedLocalityHash);
                            // From the localityHash left, pick a random one
                            if (tmpLocalities.size() > 0) {

                                int randomLocalityHashIndex = r.nextInt(tmpLocalities.size());
                                // Key all the keys associated to it
                                tmpKeys = localityKeys.get(tmpLocalities.get(randomLocalityHashIndex));
                            }
                            // Pick one key
                            keyIndex = r.nextInt(tmpKeys.size());
                            userActions[i][j] = new Action(ActionType.READ, tmpKeys.get(keyIndex));
                        }
                    } else {
                        keyIndex = r.nextInt(keys.length);
                        userActions[i][j] = new Action(ActionType.WRITE, keys[keyIndex]);
                    }
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
     * 
     *
     */
    private void printKeysLocality() {

        double average = 0;

        for (int i = 0; i < userActions.length; i++) {
            System.out.println("User " + i + " Initial location: " + userActions[i][0].key.getLocalityHash());
            System.out.print("\t Location ");
            int identical = 0;
            for (int j = 0; j < userActions[i].length; j++) {
                // System.out.print(userActions[i][j].key.getLocalityHash() + " ; ");
                if (userActions[i][j].key.getLocalityHash() == userActions[i][0].key.getLocalityHash()) {
                    identical++;
                }
            }
            System.out.println();
            System.out.println("\t Identical: " + (double) identical / (double) userActions[i].length);
            average += (double) identical / (double) userActions[i].length;
        }

        System.out.println("Total identival: " + (average / userActions.length) * 100);
    }

    /**
     * Extract all the information needed from the User threads
     */
    private void extractResults() {

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

            int nbAborts = users[i].nbBeginAborts + users[i].nbReadAborts + users[i].nbWriteAborts + users[i].nbCommitAborts;
            System.out.println("T" + users[i].userID + ":\t#readActions = " + nbReadActions + "\t#writeActions = " + nbWriteActions + "\t#aborts = " + (nbAborts));

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
        double throughput = users.length / runningTime * 1000;
        int nbAbortTotal = nbBeginAbortsTotal + nbReadAbortsTotal + nbWriteAbortsTotal + nbCommitAbortsTotal;
        double abortRate = nbAbortTotal / runningTime * 1000;

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
        System.out.println("\tThroughput: " + throughput + " transactions/s");
        System.out.println("\tAbort Rate: " + abortRate + " aborts/s");
        System.out.println("\tLocality Rate: " + localityPercentage + " %");

        printKeysLocality();

        System.out.format("#BM- %d %d %d %d %d %d %d %d %d %f %f %f %d\n", users.length, keys.length, ratio, nbReadTotal, nbReadAbortsTotal, nbWriteTotal, nbWriteAbortsTotal, nbCommitTotal, nbAbortTotal, latency, throughput, abortRate, localityPercentage);
        System.out.flush();
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

            Log.warn("T" + userID + ", is now running.");

            while (!isDone) {
                MyKey key = actions[0].key;
                UserTransaction<MyKey> t = null;

                try {

                    benchmarkStatus = BenchmarkStatus.BEGIN;
                    t = new UserTransaction<MyKey>();
                    t.begin(key);

                    Log.warn("T" + userID + ", (re)starts with transactionID = " + t.getTransactionID());

                    int alive = 0;
                    for (User u : users) {
                        if (u.isAlive())
                            alive++;
                    }
                    log.warn("Remaining alive transactions: " + alive + "/" + users.length);

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

                    Log.warn("I, T" + userID + ", am proud to announce I have been able to commit.");

                } catch (AbortException e) {

                    Log.warn("Oh no! I, T" + userID + ", have been aborted. I have to restart");

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
