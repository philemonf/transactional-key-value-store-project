package ch.epfl.tkvs.test.userclient;

import java.sql.Time;
import java.util.Random;

import ch.epfl.tkvs.transactionmanager.AbortException;
import ch.epfl.tkvs.user.Key;
import ch.epfl.tkvs.user.Transaction;


public class Benchmark implements Runnable {

    private MyKey keys[];
    private User users[];
    private int maxNbActions;

    public Benchmark(int nbKeys, int nbUsers, int maxNbActions) {
        keys = new MyKey[nbKeys];
        for (int i = 0; i < nbKeys; i++) {
            keys[i] = new MyKey("Key" + i);
        }

        this.maxNbActions = maxNbActions;

        users = new User[nbUsers];
        for (int i = 0; i < nbUsers; i++) {
            users[i] = new User(i + 1, maxNbActions);
        }

    }

    @Override
    public void run() {
        System.out.println("Benchmarking start");
        System.out.println("\t Parameters:");
        System.out.println("\t Maximum number of actions: " + maxNbActions);
        System.out.println("\t Number of keys: " + keys.length);
        System.out.println("\t Number of users: " + users.length);

        Transaction<Key> init = null;
        boolean isDone = false;

        while (!isDone) {

            isDone = true;

            try {
                init = new Transaction<Key>(new MyKey("init"));
            } catch (AbortException e) {
                e.printStackTrace();
            }

            for (int i = 0; i < keys.length; i++) {
                try {
                    init.write(keys[i], "init" + i);
                } catch (AbortException e) {
                    isDone = false;
                }
            }

            try {
                init.commit();
            } catch (AbortException e) {
                isDone = false;
            }
        }

        for (int i = 0; i < users.length; i++) {
            users[i].start();
        }

        
        int nbReadTotal=0;
        int nbWriteTotal = 0;
        int nbReadAbortsTotal = 0;
        int nbWriteAbortsTotal = 0;
        int nbCommitTotal = 0;
        double latencyTotal = 0;        
        
        for (int i = 0; i < users.length; i++) {
            try {
                users[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        
        for (int i = 0; i < users.length; i++) {
            latencyTotal += users[i].latency;
            nbReadTotal += users[i].nbRead;
            nbWriteTotal += users[i].nbWrite;
            nbWriteAbortsTotal += users[i].nbWriteAborts;
            nbReadAbortsTotal += users[i].nbReadAborts;
        }
        
        int nbAbortTotal = nbReadAbortsTotal + nbWriteAbortsTotal;
        
        System.out.println("Results:");
        System.out.println("\tNumber of Reads: " + nbReadTotal);
        System.out.println("\tNumber of Writes: " + nbWriteTotal);
        System.out.println("\tNumber of Aborts during Read: " + nbReadAbortsTotal);
        System.out.println("\tNumber of Aborts during Write: " + nbWriteAbortsTotal);
        System.out.println("\tTotal Latency: " + latencyTotal);
        System.out.println("\tTotal Commit: " + nbCommitTotal);
        System.out.println("\tTotal Aborts: " + nbAbortTotal);

        System.out.println("Benchmarking end");
    }

    private class User extends Thread {

        private int maxNbActions;
        private int userID;
        private int nbRead;
        private int nbWrite;
        private int nbReadAborts;
        private int nbWriteAborts;
        private int nbCommit;
        private double latency;

        public User(int userID, int maxNbActions) {
            this.userID = userID;
            this.maxNbActions = maxNbActions;

            this.nbRead = 0;
            this.nbWrite= 0;
            this.nbReadAborts= 0;
            this.nbWriteAborts= 0;
            this.nbCommit= 0;
            this.latency= 0;
               
        }

        @Override
        public void run() {

            latency = System.currentTimeMillis();
            // Generate a random number of Actions
            Random r = new Random();
            int nbActions = r.nextInt(maxNbActions) + 1;

            int keyIndexInit = r.nextInt(keys.length);
            MyKey key = null;
            Transaction<MyKey> t = null;

            boolean isDone = false;
            while (!isDone) {
                isDone = true;

                key = keys[keyIndexInit];
                try {
                    t = new Transaction<MyKey>(key);
                } catch (AbortException e) {
                    isDone = false;
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
                            nbWrite = nbWrite + 1;
                        } catch (AbortException e) {
                            isDone = false;
                            nbWriteAborts = nbWriteAborts + 1;
                        }

                    } else {

                        try {
                            t.read(key);
                            nbRead = nbRead+1;
                        } catch (AbortException e) {
                            isDone = false;
                            nbReadAborts= nbReadAborts+1;
                        }
                    }

                }

                try {
                    t.commit();
                    nbCommit = nbCommit +1;
                } catch (AbortException e) {
                    // If the commit was not successful, restart with a
                    // transaction which has a higher timestamp
                    isDone = false;
                }
                
                
            }
            latency = System.currentTimeMillis() - latency;
        }

        


        
        public int getNbRead() {
            return nbRead;
        }

        
        public int getNbWrite() {
            return nbWrite;
        }

        
        public int getNbReadAborts() {
            return nbReadAborts;
        }

        
        public int getNbWriteAborts() {
            return nbWriteAborts;
        }

        
        public int getNbCommit() {
            return nbCommit;
        }

        
        public double getLatency() {
            return latency;
        }
    }
}
