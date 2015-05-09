package ch.epfl.tkvs.test.userclient;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.user.UserTransaction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import junit.framework.TestCase;
import org.junit.Test;


public class UserClientScheduledTest extends TestCase {

    protected static final Logger log = Logger.getLogger(UserClientScheduledTest.class);
    public static final Boolean t = Boolean.TRUE;
    public static final Boolean f = Boolean.FALSE;
    public static int testcount = 0;

    /**
     * The class that represents a single operation to be executed by any transaction at any step.
     */
    public static abstract class TransactionExecutionCommand {

        protected Future result;

        abstract void executeBy(TransactionExecutorService t, TransactionExecutionCommand[][] schedule);

        abstract boolean checkSuccess();

    }

    /**
     * Command that does nothing.
     */
    public static TransactionExecutionCommand _____________ = new TransactionExecutionCommand() {

        @Override
        boolean checkSuccess() {
            return true;
        }

        @Override
        void executeBy(TransactionExecutorService t, TransactionExecutionCommand[][] schedule) {

        }
    };

    /**
     * Used to wait for any particular operation of another transaction, in case schedule synchronization is
     * insufficient.
     * @param tid Id of the transaction
     * @param step The step of the given transaction on which we should wait
     * @return
     */
    public static TransactionExecutionCommand WAITFOR(final int tid, final int step) {
        return new TransactionExecutionCommand() {

            @Override
            public String toString() {
                return "Wait for step " + step + "  of transaciton " + tid;
            }

            @Override
            void executeBy(TransactionExecutorService t, TransactionExecutionCommand[][] schedule) {
                Future past = schedule[tid][step].result;
                result = t.waitFor(past);
            }

            @Override
            boolean checkSuccess() {
                try {
                    result.get();
                    return true;
                } catch (InterruptedException | ExecutionException ex) {
                    log.error("Wait failed", ex);
                    return false;
                }
            }
        };
    }

    /**
     * Command to begin a transaction
     * @param hash The hash of the key which determines the primary Transaction Manager for this transaction
     * @param expectedResult t, if the operation should succeed, f otherwise. Objects t and f are already defined
     * @return
     */
    public static TransactionExecutionCommand BEGIN__(final int hash, final Boolean expectedResult) {

        return new TransactionExecutionCommand() {

            @Override
            public String toString() {
                return "Begin: hash=" + hash;
            }

            @Override
            boolean checkSuccess() {

                try {
                    result.get();
                    return expectedResult;
                } catch (Exception ex) {
                    if (ex.getCause() instanceof AbortException && expectedResult == Boolean.FALSE) {
                        return true;
                    }
                    log.error("Exception at runtime : ", ex);
                }
                return false;
            }

            @Override
            void executeBy(TransactionExecutorService t, TransactionExecutionCommand[][] schedule) {
                result = t.begin(new MyKey("init", hash));
            }
        };

    }

    /**
     * Command to commit a transaction
     * @param expectedResult t, if the operation should succeed, f otherwise. Objects t and f are already defined
     * @return
     */
    public static TransactionExecutionCommand COMMIT____(final Boolean expectedResult) {

        return new TransactionExecutionCommand() {

            @Override
            public String toString() {
                return "Commit";
            }

            @Override
            boolean checkSuccess() {

                try {
                    result.get();
                    return expectedResult;
                } catch (Exception ex) {
                    if (ex.getCause() instanceof AbortException && expectedResult == Boolean.FALSE) {
                        return true;
                    }
                    log.error("Exception at runtime : ", ex);
                }
                return false;
            }

            @Override
            void executeBy(TransactionExecutorService t, TransactionExecutionCommand[][] schedule) {
                result = t.commit();
            }
        };

    }

    /**
     * Command to read value of a key.
     * @param key The key whose value is to be read
     * @param expectedResult The expected value if the operation should succeed, null if the operation should fail
     * @return
     */
    public static TransactionExecutionCommand READ(final MyKey key, final String expectedResult

    ) {
        return new TransactionExecutionCommand() {

            @Override
            public String toString() {
                return "Read: " + key;
            }

            @Override
            boolean checkSuccess() {

                try {
                    String r = (String) result.get();
                    return r.equals(expectedResult);
                } catch (Exception ex) {
                    if (ex.getCause() instanceof AbortException && expectedResult == null) {
                        return true;
                    }
                    log.error("Exception at runtime : ", ex);
                }
                return false;
            }

            @Override
            void executeBy(TransactionExecutorService t, TransactionExecutionCommand[][] schedule) {
                result = t.read(key);
            }
        };

    }

    /**
     * Command to write a value for a particular key
     * @param key The key for which value is to be written
     * @param value The value to be written
     * @param expectedResult t, if the operation should succeed, f otherwise. Objects t and f are already defined
     * @return
     */
    public static TransactionExecutionCommand W(final MyKey key, final String value, final Boolean expectedResult) {

        return new TransactionExecutionCommand() {

            @Override
            public String toString() {
                return "Write: " + key + "  value=" + value;
            }

            @Override
            boolean checkSuccess() {

                try {
                    result.get();
                    return expectedResult;
                } catch (Exception ex) {
                    if (ex.getCause() instanceof AbortException && expectedResult == Boolean.FALSE) {
                        return true;
                    }
                    log.error("Exception at runtime : ", ex);
                }
                return false;
            }

            @Override
            void executeBy(TransactionExecutorService t, TransactionExecutionCommand[][] schedule) {
                result = t.write(key, value);
            }
        };

    }

    /**
     * Generates fresh key required for each test case
     * @param key
     * @param hash
     * @return
     */
    public MyKey freshKey(String key, int hash) {
        return new MyKey("Test" + testcount + "::" + key, hash);
    }

    /**
     * Executes given schedule after initializing keys with default values and tests if the operations performed as
     * expected. A transaction is represented by an array of TransactionExecutionCommands, each of which is guaranteed
     * to run one after the other. A schedule is an array of transaction possibly of different lengths. The executor
     * first creates transaction t0 which initializes the mentioned gives to default value 00 Then the executor proceeds
     * step by step where in each step, 1. It executes the next command, if any, for all transactions 2. Waits for some
     * time to allow each operation to either complete or get blocked
     * @param schedule
     * @param keysToBeInit
     */
    public void execute(TransactionExecutionCommand[][] schedule, MyKey... keysToBeInit) {
        int numTrans = schedule.length;

        TransactionExecutorService t0 = new TransactionExecutorService();
        TransactionExecutorService[] tes = new TransactionExecutorService[numTrans];
        for (int i = 0; i < numTrans; i++) {
            tes[i] = new TransactionExecutorService();
        }
        int step = 0;
        t0.begin(new MyKey("init", 0));
        try {
            for (MyKey k : keysToBeInit) {
                t0.write(k, "00");
            }
            t0.commit();
            t0.executor.shutdown();
            t0.executor.awaitTermination(5, TimeUnit.MINUTES);

            boolean canExecuteFurther;
            do {
                canExecuteFurther = false;
                for (int i = 0; i < numTrans; i++) {
                    if (step < schedule[i].length) {
                        canExecuteFurther = true;
                        TransactionExecutionCommand command = schedule[i][step];
                        if (command != _____________) {
                            System.out.println("@t" + i + " :: " + step + "     " + command);
                            command.executeBy(tes[i], schedule);
                        }
                    } else {
                        tes[i].executor.shutdown();
                    }
                }
                step++;
                Thread.sleep(1000); // Wait for operations to terminate or block
            } while (canExecuteFurther);
            for (int s = 0; s < step; s++) {
                for (int i = 0; i < numTrans; i++) {
                    if (s < schedule[i].length && !schedule[i][s].checkSuccess()) {
                        fail("Test failed at step " + s + " for transaction " + (i + 1));
                    }
                }

            }
        } catch (Exception ex) {

            ex.printStackTrace();
        } finally {
            testcount++;
            t0.executor.shutdown();
            for (int i = 0; i < numTrans; i++) {
                tes[i].executor.shutdown();
            }
        }
    }

    private static class TransactionExecutorService {

        ExecutorService executor;
        UserTransaction<MyKey> t;

        public TransactionExecutorService() {
            executor = Executors.newSingleThreadExecutor();
            t = new UserTransaction<>();

        }

        public Future begin(final MyKey k) {
            return executor.submit(new Callable() {

                @Override
                public Object call() throws Exception {
                    t.begin(k);
                    return null;
                }

            });
        }

        public Future<String> read(final MyKey k) {
            return executor.submit(new Callable<String>() {

                @Override
                public String call() throws Exception {
                    return (String) t.read(k);
                }
            });
        }

        public Future write(final MyKey k, final String value) {
            return executor.submit(new Callable() {

                @Override
                public Object call() throws Exception {
                    t.write(k, value);
                    return null;
                }
            });
        }

        public Future commit() {
            return executor.submit(new Callable() {

                @Override
                public Object call() throws Exception {
                    t.commit();
                    return null;
                }
            });
        }

        public Future waitFor(final Future past) {
            return executor.submit(new Callable() {

                @Override
                public Object call() throws Exception {
                    past.get();
                    return null;
                }
            });
        }
    }

    @Test
    public void testSingle() {
        MyKey x = freshKey("x", 0);
        TransactionExecutionCommand schedule[][] = {

        /* T1 */{ BEGIN__(0, t), W(x, "x0", t), READ(x, "x0"), COMMIT____(t) } };
        execute(schedule);
    }

    @Test
    public void testSerial() {
        MyKey x = freshKey("x", 0);
        MyKey y = freshKey("y", 1);
        TransactionExecutionCommand schedule[][] = {
        /* T1 */{ BEGIN__(0, t), W(x, "x0", t), READ(x, "x0"), COMMIT____(t), _____________, },
        /* T2 */{ _____________, _____________, _____________, _____________, WAITFOR(0, 3), BEGIN__(1, t), W(y, "y1", t), READ(x, "x0"), READ(y, "y1"), COMMIT____(t) } };
        execute(schedule);

    }

}
