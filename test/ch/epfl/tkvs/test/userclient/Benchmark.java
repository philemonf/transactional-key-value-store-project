package ch.epfl.tkvs.test.userclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.user.Key;
import ch.epfl.tkvs.user.UserTransaction;


/**
 * Configurable Random Benchmark Framework
 * 
 * Statically configured (for now in Client.java): #nodes, max #keys, locality percentage
 * 
 * Dynamically configured with Benchmark's constructor: #keys (must be less than max #keys), #transactions, #requests
 * per transactions, read:write ratio, #repetitions (results are averaged across #repetitions)
 * 
 * The benchmarks assume that locality hashes 0..(N-1) are mapped to nodes 1..N when there are at least N nodes.
 */
public class Benchmark {

    private static Logger log = Logger.getLogger(Benchmark.class.getName());

    /** Keys the users will access (are written once at the beginning) */
    private static MyKey allKeys[];
    /** Percentage of keys in a transaction which are on the same node of the key used to create the transaction */
    private static int localityPercentage;
    /** Indicate if the locality is used for the benchmark */
    private static boolean localityIsUsed;
    /** Number of nodes */
    private static int nbNodes;

    /**
     * Write all keys that any benchmark can use to the Key-Value Store
     * @throws AbortException in case of any problem writing a given key
     */
    public static void initializeKeys(int nbNodes, int maxNbKeys, int localityPercentage) throws AbortException {

        if (0 <= localityPercentage && localityPercentage <= 100) {
            Benchmark.localityPercentage = localityPercentage;
            Benchmark.localityIsUsed = true;
        } else {
            Benchmark.localityPercentage = 0;
            Benchmark.localityIsUsed = false;
        }

        // Create all keys
        Benchmark.nbNodes = nbNodes;
        Benchmark.allKeys = new MyKey[maxNbKeys];

        for (int i = 0; i < maxNbKeys; i++) {
            // Create a key ans associate a random locality node with it
            Benchmark.allKeys[i] = new MyKey("Key" + i, i % Benchmark.nbNodes);
        }

        // Write all keys to the cluster
        UserTransaction<Key> init = null;
        init = new UserTransaction<Key>();
        init.begin(Benchmark.allKeys[new Random().nextInt(Benchmark.allKeys.length)]);

        for (int i = 0; i < Benchmark.allKeys.length; i++) {
            init.write(Benchmark.allKeys[i], "init" + i);
        }

        init.commit();
    }

    /** #keys used by this benchmark */
    private int nbKeys;
    /** keys used by this benchmark sorted by the node they are on */
    private HashMap<Integer, ArrayList<MyKey>> usedKeysFromNode;

    /** Users threads used by this benchmark that represent a user doing one transaction */
    private User users[];

    /** Maximal number of actions for a user */
    private int maxNbActions;
    /** Ratio of #reads compared to one write */
    private int ratio;

    /** Repeat the benchmark multiple times and average the results */
    private int repetitions;

    /** Results of the benchmark (or average results across repetitions) */
    private Results results;

    private ConcurrentFIFO latencyBuffer;

    /**
     * @param nbKeys: Number of keys that will be accessed by the users
     * @param nbUsers: Number of users = transactions for the benchmark
     * @param maxNbActions: Max number of actions that will be done by a user
     * @param ratio: Number of read for one write
     * @param repetition: Number of time the benchmark will be repeated with the same parameters
     */
    public Benchmark(int nbKeys, int nbUsers, int maxNbActions, int ratio, int repetitions) {

        this.nbKeys = nbKeys;
        this.users = new User[nbUsers];

        this.maxNbActions = maxNbActions;
        this.ratio = ratio;
        this.repetitions = repetitions;

        int fivePercentNbTransactions = (int) (0.05 * nbUsers);
        if (fivePercentNbTransactions < 1) {
            fivePercentNbTransactions = 1;
        }
        this.latencyBuffer = new ConcurrentFIFO(fivePercentNbTransactions);

        results = new Results();
    }

    /** Run the benchmark and output results to stdout */
    public void run() {

        log.info("Benchmarking starts for " + users.length + " transactions");

        for (int i = 0; i < repetitions; i++) {
            initializeListOfUsedKeys();
            initializeUsersWithRandomActions();
            long runningTime = startBenchmark();
            extractAndAddResults(runningTime);
            log.info("Repetition " + (i + 1) + " done");
        }

        System.out.format("#BM-\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%f\t%f\t%f\n", Benchmark.nbNodes, users.length, maxNbActions, nbKeys, Benchmark.localityPercentage, ratio, repetitions, results.throughput / repetitions, results.latency / repetitions, results.abortRate / repetitions);
        System.out.flush();
    }

    /** Choose nbKeys keys from allKeys and initialize the locality-based list of keys */
    private void initializeListOfUsedKeys() {

        usedKeysFromNode = new HashMap<Integer, ArrayList<MyKey>>();

        for (int i = 0; i < nbKeys; ++i) {
            MyKey keyToAdd = allKeys[i];
            if (usedKeysFromNode.get(keyToAdd.getLocalityHash()) == null) {
                usedKeysFromNode.put(keyToAdd.getLocalityHash(), new ArrayList<MyKey>());
            }
            usedKeysFromNode.get(keyToAdd.localityHash).add(keyToAdd);
        }
    }

    /** Initialize the list of actions the users will execute in the benchmark */
    private void initializeUsersWithRandomActions() {

        Random rand = new Random();

        for (int userIndex = 0; userIndex < users.length; userIndex++) {

            // Create a new user with a random locality hint 0..(N-1) where N is #nodes
            users[userIndex] = new User(userIndex, rand.nextInt(nbNodes));

            // Generate a random number of Actions
            int nbActions = rand.nextInt(maxNbActions) + 1;
            users[userIndex].actions = new Action[nbActions];

            for (int i = 0; i < nbActions; i++) {
                // Determine if we want to read or write a key
                int write = rand.nextInt(ratio);
                if (write != 0) {
                    users[userIndex].actions[i] = new Action(Action.ActionType.READ, getRandomKey(users[userIndex].localityHint));
                } else {
                    users[userIndex].actions[i] = new Action(Action.ActionType.WRITE, getRandomKey(users[userIndex].localityHint));
                }
            }
        }
    }

    private MyKey getRandomKey(int localNode) {

        Random r = new Random();

        // If locality is not used then return a random key
        if (!localityIsUsed) {
            return allKeys[r.nextInt(nbKeys)];
        }

        // If locality is used, choose randomly whether the key is local or remote
        boolean keyIsLocal = r.nextInt(100) < localityPercentage;

        if (keyIsLocal) {
            // return random local key
            ArrayList<MyKey> localKeys = usedKeysFromNode.get(localNode);
            return localKeys.get(r.nextInt(localKeys.size()));
        } else {
            // return a random remote key by first choosing a random remote node and then a random key on this node
            int randomRemoteNode = r.nextInt(Math.max(1, nbNodes - 1));
            if (randomRemoteNode >= localNode)
                randomRemoteNode++;

            ArrayList<MyKey> remoteKeysOnRandomRemoteNode = usedKeysFromNode.get(randomRemoteNode);
            return remoteKeysOnRandomRemoteNode.get(r.nextInt(remoteKeysOnRandomRemoteNode.size()));
        }
    }

    /**
     * Initialize the Users threads by specifying their actions Start the threads to run concurrently Wait for all the
     * threads to stop
     * @return benchmark's running time
     */
    private long startBenchmark() {

        long runningTime = System.currentTimeMillis();

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
        return runningTime;
    }

    /**
     * Extract all the information needed from the User threads
     */
    private void extractAndAddResults(long runningTime) {

        for (User user : users) {
            results.nbBeginAbortsTotal += user.nbBeginAborts;
            results.nbReadAbortsTotal += user.nbReadAborts;
            results.nbWriteAbortsTotal += user.nbWriteAborts;
            results.nbCommitAbortsTotal += user.nbCommitAborts;
        }

        results.throughput += ((double) users.length) / runningTime * 1000.0;
        results.nbAbortTotal = results.nbBeginAbortsTotal + results.nbReadAbortsTotal + results.nbWriteAbortsTotal + results.nbCommitAbortsTotal;
        results.abortRate += ((double) results.nbAbortTotal) / runningTime * 1000.0;

        double latencyFivePercentSlowest = 0;
        for (int i = 0; i < latencyBuffer.size(); i++) {
            latencyFivePercentSlowest += latencyBuffer.get(i);
        }
        results.latency += latencyFivePercentSlowest / latencyBuffer.size();
    }

    private class Results {

        /** troughput in transactions/s */
        double throughput = 0;

        /** latency of the 5% slower transactions ms/transaction */
        double latency = 0;

        /** Total #aborts */
        int nbAbortTotal = 0;
        /** Abort rate in aborts/s */
        double abortRate = 0;

        /** Total #aborts due to begin requests */
        int nbBeginAbortsTotal = 0;
        /** Total #aborts due to read requests */
        int nbReadAbortsTotal = 0;
        /** Total #aborts due to write requests */
        int nbWriteAbortsTotal = 0;
        /** Total #aborts due to commit requests */
        int nbCommitAbortsTotal = 0;
    }

    private static class Action {

        private static enum ActionType {
            WRITE, READ
        }

        private MyKey key;
        private ActionType type;

        /**
         * @param type of the Action (WRITE or READ)
         * @param key id
         */
        public Action(ActionType type, MyKey key) {

            this.type = type;
            this.key = key;
        }
    }

    private static enum BenchmarkStatus {
        BEGIN, READ, WRITE, COMMIT
    }

    private class User extends Thread {

        // ID of the user
        private int userID;
        // 0..(N-1) where N is #nodes
        private int localityHint;
        // Contains the actions of the users in order
        private Action actions[];

        // Number of aborts generated by a begin
        private int nbBeginAborts = 0;
        // Number of aborts generated by a read
        private int nbReadAborts = 0;
        // Number of aborts generated by a write
        private int nbWriteAborts = 0;
        // Number of aborts generated by a commit
        private int nbCommitAborts = 0;
        // Number of commits
        private int nbCommit = 0;

        /**
         * @param userID
         * @param actions
         */
        public User(int userID, int localityHint) {

            this.userID = userID;
            this.localityHint = localityHint;
        }

        @Override
        public void run() {

            BenchmarkStatus benchmarkStatus = BenchmarkStatus.BEGIN;
            boolean isDone = false;
            long latency = System.currentTimeMillis();

            while (!isDone) {
                // Get any key that is a local key
                MyKey localityHintKey = usedKeysFromNode.get(localityHint).get(0);
                UserTransaction<MyKey> t = null;

                try {

                    benchmarkStatus = BenchmarkStatus.BEGIN;
                    t = new UserTransaction<MyKey>();
                    t.begin(localityHintKey);

                    for (int i = 0; i < actions.length; i++) {
                        MyKey key = actions[i].key;
                        switch (actions[i].type) {
                        case WRITE:
                            benchmarkStatus = BenchmarkStatus.WRITE;
                            t.write(key, "UserID:" + userID + " Action:" + i);
                            break;
                        case READ:
                            benchmarkStatus = BenchmarkStatus.READ;
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
            latencyBuffer.add(latency);
        }
    }
}
