package ch.epfl.tkvs.test.userclient;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.user.UserTransaction;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.TestCase;


public class UserClientScheduledTest extends TestCase {

    public static abstract class TransactionExecutionCommand {

        Method m;
        List<Object> args;
        Future result;
        Object expectedResult;

        public TransactionExecutionCommand(Method m, List<Object> args, Object expectedResult) {
            this.m = m;
            this.args = args;
        }

        abstract boolean checkSuccess();

    }

    public static TransactionExecutionCommand B(String key, int hash, Boolean expectedResult) {
        try {
            Method m = TransactionExecutorService.class.getDeclaredMethod("begin", MyKey.class);
            List<Object> args = Arrays.asList((Object) new MyKey(key, hash));
            return new TransactionExecutionCommand(m, args, expectedResult) {

                @Override
                boolean checkSuccess() {

                    try {
                        result.get();
                        return true;
                    } catch (InterruptedException ex) {
                        Logger.getLogger(UserClientScheduledTest.class.getName()).log(Level.SEVERE, null, ex);

                    } catch (ExecutionException ex) {
                        if (ex.getCause() instanceof AbortException && expectedResult == Boolean.FALSE)
                            return true;

                    }
                    return false;
                }
            };
        } catch (Exception ex) {
            return null;
        }
    }

    public static TransactionExecutionCommand C(Boolean expectedResult) {
        try {
            Method m = TransactionExecutorService.class.getDeclaredMethod("commit");
            List<Object> args = null;
            return new TransactionExecutionCommand(m, args, expectedResult) {

                @Override
                boolean checkSuccess() {

                    try {
                        result.get();
                        return true;
                    } catch (InterruptedException ex) {
                        Logger.getLogger(UserClientScheduledTest.class.getName()).log(Level.SEVERE, null, ex);

                    } catch (ExecutionException ex) {
                        if (ex.getCause() instanceof AbortException && expectedResult == Boolean.FALSE)
                            return true;

                    }
                    return false;
                }
            };
        } catch (Exception ex) {
            return null;
        }
    }

    public static TransactionExecutionCommand R(String key, int hash, String expectedResult) {
        try {
            Method m = TransactionExecutorService.class.getDeclaredMethod("read", MyKey.class);
            List<Object> args = Arrays.asList((Object) new MyKey(key, hash));
            return new TransactionExecutionCommand(m, args, expectedResult) {

                @Override
                boolean checkSuccess() {

                    try {
                        String r = (String) result.get();
                        return r.equals(expectedResult);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(UserClientScheduledTest.class.getName()).log(Level.SEVERE, null, ex);

                    } catch (ExecutionException ex) {
                        if (ex.getCause() instanceof AbortException && expectedResult == null)
                            return true;

                    }
                    return false;
                }
            };
        } catch (Exception ex) {
            return null;
        }

    }

    public static TransactionExecutionCommand W(String key, int hash, String value, Boolean expectedResult) {
        try {
            Method m = TransactionExecutorService.class.getDeclaredMethod("write", MyKey.class, String.class);
            List<Object> args = Arrays.asList((Object) new MyKey(key, hash), (Object) value);
            return new TransactionExecutionCommand(m, args, expectedResult) {

                @Override
                boolean checkSuccess() {

                    try {
                        result.get();
                        return true;
                    } catch (InterruptedException ex) {
                        Logger.getLogger(UserClientScheduledTest.class.getName()).log(Level.SEVERE, null, ex);

                    } catch (ExecutionException ex) {
                        if (ex.getCause() instanceof AbortException && expectedResult == Boolean.FALSE)
                            return true;

                    }
                    return false;
                }
            };
        } catch (Exception ex) {
            return null;
        }

    }

    void execute(TransactionExecutionCommand[][] schedule) {
        int numTrans = schedule.length;
        TransactionExecutorService[] tes = new TransactionExecutorService[numTrans];
        for (int i = 0; i < numTrans; i++)
            tes[i] = new TransactionExecutorService();
        int step = 0;
        boolean canExecuteFurther;
        do {
            canExecuteFurther = false;
            for (int i = 0; i < numTrans; i++) {
                if (step < schedule[i].length) {
                    canExecuteFurther = true;
                    Method m = schedule[i][step].m;
                    List<Object> args = schedule[i][step].args;
                    try {
                        schedule[i][step].result = (Future) m.invoke(tes[i], args);
                    } catch (Exception ex) {
                        fail(ex.getMessage());
                    }
                }
            }
            step++;
        } while (canExecuteFurther);
        for (int s = 0; s < step; s++) {
            for (int i = 0; i < numTrans; i++)
                if (!schedule[i][s].checkSuccess())
                    fail("Test failed at step " + s + " for transaction " + i);

        }
    }

    public static class TransactionExecutorService {

        ExecutorService executor;
        UserTransaction<MyKey> t;

        public TransactionExecutorService() {
            executor = Executors.newSingleThreadExecutor();
            t = new UserTransaction<>();

        }

        private Future begin(final MyKey k) {
            return executor.submit(new Callable() {

                @Override
                public Object call() throws Exception {
                    t.begin(k);
                    return null;
                }

            });
        }

        private Future<String> read(final MyKey k) {
            return executor.submit(new Callable<String>() {

                @Override
                public String call() throws Exception {
                    return (String) t.read(k);
                }
            });
        }

        private Future write(final MyKey k, final String value) {
            return executor.submit(new Callable() {

                @Override
                public Object call() throws Exception {
                    t.write(k, value);
                    return null;
                }
            });
        }

        private Future commit() {
            return executor.submit(new Callable() {

                @Override
                public Object call() throws Exception {
                    t.commit();
                    return null;
                }
            });
        }
    }

}
