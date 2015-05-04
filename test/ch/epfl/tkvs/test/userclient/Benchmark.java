package ch.epfl.tkvs.test.userclient;

import java.util.Random;

import ch.epfl.tkvs.transactionmanager.AbortException;
import ch.epfl.tkvs.user.Key;
import ch.epfl.tkvs.user.Transaction;


public class Benchmark implements Runnable {

    private MyKey keys[];
    private Thread users[];
    private int maxNbActions;

    public Benchmark(int nbKeys, int nbUsers, int maxNbActions) {
        keys = new MyKey[nbKeys];
        for (int i = 0; i < nbKeys; i++) {
            keys[i] = new MyKey("Key" + i);
        }

        this.maxNbActions = maxNbActions;

        users = new Thread[nbUsers];
        for (int i = 0; i < nbUsers; i++) {
            users[i] = new Thread(new User(i + 1, maxNbActions));
        }

    }

    @Override
    public void run() {
        System.out.println("Benchmarking start");
        System.out.println("\t Parameters:");
        System.out.println("\t\t Maximum number of actions: " + maxNbActions);
        System.out.println("\t\t Number of keys: " + keys.length);
        System.out.println("\t\t Number of users: " + users.length);

        Transaction<Key> init = null;
        boolean isDone = false;

        while (!isDone) {
            try {
                init = new Transaction<Key>(new MyKey("init"));
            } catch (AbortException e) {
                e.printStackTrace();
            }

            for (int i = 0; i < keys.length; i++) {
                try {
                    init.write(keys[i], "init" + i);
                } catch (AbortException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            try {
                init.commit();
                isDone = true;
            } catch (AbortException e) {
                e.printStackTrace();
                isDone = false;
            }
        }

        for (int i = 0; i < users.length; i++) {
            users[i].start();
        }

        for (int i = 0; i < users.length; i++) {
            try {
                users[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Benchmarking end");
    }

    private class User implements Runnable {

        private int maxNbActions;
        private int userID;

        public User(int userID, int maxNbActions) {
            this.userID = userID;
            this.maxNbActions = maxNbActions;

        }

        @Override
        public void run() {

            // Generate a random number of Actions
            Random r = new Random();
            int nbActions = r.nextInt(maxNbActions) + 1;

            int keyIndexInit = r.nextInt(keys.length);
            MyKey key = null;
            Transaction<MyKey> t = null;

            boolean isDone = false;
            while (!isDone) {
                key = keys[keyIndexInit];
                try {
                    t = new Transaction<MyKey>(key);
                } catch (AbortException e) {
                    e.printStackTrace();
                }

                for (int i = 0; i < nbActions; i++) {
                    int keyIndex = 0;
                    keyIndex = r.nextInt(keys.length);
                    key = keys[keyIndex];

                    // Determine if we want to read or write a key
                    boolean isWrite = r.nextBoolean();
                    if (isWrite) {

                        try {
                            t.write(key, "UserID:" + userID + " Action:" + i);
                        } catch (AbortException e) {
                            e.printStackTrace();
                        }

                    } else {

                        try {
                            t.read(key);
                        } catch (AbortException e) {
                            e.printStackTrace();
                        }
                    }

                }

                try {
                    t.commit();
                    isDone = true;
                } catch (AbortException e) {
                    // If the commit was not successful, restart with a
                    // transaction which has a higher timestamp
                    isDone = false;
                    e.printStackTrace();
                }
            }

        }
    }
}
