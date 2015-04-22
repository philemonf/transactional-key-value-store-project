package ch.epfl.tkvs.transactionmanager.lockingunit;

import ch.epfl.tkvs.transactionmanager.AbortException;
import java.io.Serializable;
import java.util.Arrays;
import static java.util.Arrays.asList;
import java.util.HashMap;
import java.util.List;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.Test;


public class LockingUnitTest extends TestCase {

    private static boolean check = true;

    private HashMap<Serializable, List<LockType>> hashMapify(Serializable key, LockType lock) {
        HashMap<Serializable, List<LockType>> locks = new HashMap<>();

        locks.put(key, Arrays.asList(lock));
        return locks;

    }

    @Test
    public void testExclusiveLock() throws Exception {
        LockingUnit.instance.initOnlyExclusiveLock();
        check = false;

        Thread thread1 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LockingUnit.instance.lock(1, "test", LockType.Exclusive.LOCK);
                    Thread.sleep(1000);

                    check = true;

                    LockingUnit.instance.releaseAll(1, hashMapify("test", LockType.Exclusive.LOCK));
                } catch (InterruptedException | AbortException ex) {
                    fail("Abort");
                }
            }
        });

        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LockingUnit.instance.lock(2, "test", LockType.Exclusive.LOCK);
                    assertEquals("Thread1 did not block Thread2", check, true);
                    LockingUnit.instance.releaseAll(2, hashMapify("test", LockType.Exclusive.LOCK));
                } catch (AbortException ex) {
                    fail("Abort");
                }
            }
        });

        thread1.start();
        Thread.sleep(500);
        thread2.start();

        thread1.join();
        thread2.join();
        assertEquals(check, true);
    }

    @Test
    public void testDefaultLockRead() throws Exception {
        // Init with default parameters
        LockingUnit.instance.init();
        final Semaphore sem = new Semaphore(0);

        Thread thread1 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LockingUnit.instance.lock(1, "test", LockType.Default.READ_LOCK);
                    LockingUnit.instance.lock(1, "test11", LockType.Default.READ_LOCK);
                    if (!sem.tryAcquire(1, 1000, TimeUnit.MILLISECONDS)) {
                        fail("Thread2 did not release the semaphore and was blocked on the read lock");
                    }

                    LockingUnit.instance.releaseAll(1, hashMapify("test11", LockType.Default.READ_LOCK));
                    LockingUnit.instance.releaseAll(1, hashMapify("test", LockType.Default.READ_LOCK));
                } catch (AbortException | InterruptedException ex) {
                    fail("Abort");
                }
            }
        });

        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LockingUnit.instance.lock(2, "test22", LockType.Default.READ_LOCK);
                    LockingUnit.instance.lock(2, "test", LockType.Default.READ_LOCK);
                    sem.release();
                    LockingUnit.instance.releaseAll(2, hashMapify("test", LockType.Default.READ_LOCK));
                    LockingUnit.instance.releaseAll(2, hashMapify("test22", LockType.Default.READ_LOCK));
                } catch (AbortException ex) {
                    fail("Abort");
                }
            }
        });

        thread1.start();
        Thread.sleep(500);
        thread2.start();

        thread1.join();
        thread2.join();
    }

    @Test
    public void testDefaultLockWrite() throws Exception {
        // Init with default parameters
        LockingUnit.instance.init();

        final Semaphore sem = new Semaphore(0);
        Thread thread1 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LockingUnit.instance.lock(1, "test", LockType.Default.WRITE_LOCK);

                    if (sem.tryAcquire(1, 1000, TimeUnit.MILLISECONDS)) {
                        fail("Thread2 could release the semaphore and was not blocked on the write lock");
                    }
                } catch (AbortException | InterruptedException e) {
                    fail();
                }
                LockingUnit.instance.releaseAll(1, hashMapify("test", LockType.Default.WRITE_LOCK));
            }
        });

        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LockingUnit.instance.lock(2, "test", LockType.Default.WRITE_LOCK);
                    sem.release();
                    LockingUnit.instance.releaseAll(2, hashMapify("test", LockType.Default.WRITE_LOCK));
                } catch (AbortException ex) {
                    fail();
                }
            }
        });

        thread1.start();
        Thread.sleep(500);
        thread2.start();

        thread1.join();
        thread2.join();
    }

    @Test
    public void testDefaultLockPromote() throws Exception {
        LockingUnit.instance.init();
        final Semaphore sem = new Semaphore(0);
        final Semaphore t1Started = new Semaphore(0);

        Thread thread1 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LockingUnit.instance.lock(1, "test", LockType.Default.READ_LOCK);
                    LockingUnit.instance.promote(1, "test", asList(LockType.Default.READ_LOCK), LockType.Default.WRITE_LOCK);
                    t1Started.release();

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }

                    try {
                        if (sem.tryAcquire(1, 1000, TimeUnit.MILLISECONDS)) {
                            fail("Thread2 released the semaphore during promotion!");
                        }
                    } catch (InterruptedException e) {
                        fail();
                    }
                    LockingUnit.instance.releaseAll(1, hashMapify("test", LockType.Default.WRITE_LOCK));
                } catch (AbortException ex) {
                    fail();
                }
            }
        });

        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LockingUnit.instance.lock(2, "test", LockType.Default.READ_LOCK);
                    sem.release();
                    LockingUnit.instance.releaseAll(2, hashMapify("test", LockType.Default.READ_LOCK));
                } catch (AbortException ex) {
                    fail();
                }
            }
        });

        thread1.start();
        t1Started.acquire();
        thread2.start();

        thread1.join();
        thread2.join();
    }

    @Test
    public void testDefaultLockPromote2() throws Exception {
        LockingUnit.instance.init();
        final Semaphore t2AcquiredTheLockSem = new Semaphore(0);
        final Semaphore t2ReleasedTheLockSem = new Semaphore(0);

        Thread thread1 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LockingUnit.instance.lock(1, "test", LockType.Default.READ_LOCK);

                    t2AcquiredTheLockSem.acquire();
                    LockingUnit.instance.promote(1, "test", asList(LockType.Default.READ_LOCK), LockType.Default.WRITE_LOCK);

                    if (!t2ReleasedTheLockSem.tryAcquire()) {
                        fail("Thread1 could promote while t2 had the lock.");
                    }
                } catch (InterruptedException | AbortException e1) {
                    fail();
                }
                LockingUnit.instance.releaseAll(1, hashMapify("test", LockType.Default.WRITE_LOCK));
            }
        });

        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LockingUnit.instance.lock(2, "test", LockType.Default.READ_LOCK);
                    t2AcquiredTheLockSem.release();

                    Thread.sleep(1500);

                    LockingUnit.instance.releaseAll(2, hashMapify("test", LockType.Default.READ_LOCK));
                    t2ReleasedTheLockSem.release();
                } catch (InterruptedException | AbortException e) {
                    fail();
                }
            }
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();
    }

}
