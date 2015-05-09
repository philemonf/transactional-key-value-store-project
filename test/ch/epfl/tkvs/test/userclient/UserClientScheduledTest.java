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

    private static final Logger log = Logger.getLogger(UserClientScheduledTest.class);
    public static final Boolean t = Boolean.TRUE;
    public static final Boolean f = Boolean.FALSE;
    public static int testcount = 0;

    public static abstract class TransactionExecutionCommand {

        protected Future result;

        abstract void executeBy(TransactionExecutorService t, TransactionExecutionCommand[][] schedule);

        abstract boolean checkSuccess();

    }

    public static TransactionExecutionCommand __________________ = new TransactionExecutionCommand() {

        @Override
        boolean checkSuccess() {
            return true;
        }

        @Override
        void executeBy(TransactionExecutorService t, TransactionExecutionCommand[][] schedule) {

        }
    };

    public static TransactionExecutionCommand WAITFOR(final int tid, final int step) {
        return new TransactionExecutionCommand() {

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

    public static TransactionExecutionCommand BEGIN_______(final int hash, final Boolean expectedResult) {

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

    public static TransactionExecutionCommand COMMIT_________(final Boolean expectedResult) {

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

    public MyKey freshKey(String key, int hash) {
        return new MyKey("Test" + testcount + "::" + key, hash);
    }

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
                t0.write(k, "init");
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
                        if (command != __________________) {
                            System.out.println("@t" + i + " :: " + step + "     " + command);
                            command.executeBy(tes[i], schedule);
                        }
                    } else {
                        tes[i].executor.shutdown();
                    }
                }
                step++;
            } while (canExecuteFurther);
            for (int s = 0; s < step; s++) {
                for (int i = 0; i < numTrans; i++) {
                    if (s < schedule[i].length && !schedule[i][s].checkSuccess()) {
                        fail("Test failed at step " + s + " for transaction " + i);
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

    public static class TransactionExecutorService {

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

        /* T1 */{ BEGIN_______(0, t), W(x, "x0", t), READ(x, "x0"), COMMIT_________(t) } };
        execute(schedule);
    }

    @Test
    public void testSerial() {
        MyKey x = freshKey("x", 0);
        MyKey y = freshKey("y", 1);
        TransactionExecutionCommand schedule[][] = {
        /* T1 */
        { BEGIN_______(0, t), W(x, "x0", t), READ(x, "x0"), COMMIT_________(t) },
        /* T2 */{ __________________, __________________, __________________, __________________, WAITFOR(0, 3), BEGIN_______(1, t), W(y, "y1", t), READ(x, "x0"), READ(y, "y1"), COMMIT_________(t) } };
        execute(schedule);

    }

}
