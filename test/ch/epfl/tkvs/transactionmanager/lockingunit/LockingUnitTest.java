package ch.epfl.tkvs.transactionmanager.lockingunit;

import static java.util.Arrays.asList;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.Test;


public class LockingUnitTest extends TestCase {

    private static boolean check = true;

    @Test
    public void testExclusiveLock() throws Exception {
        LockingUnit.instance.initOnlyExclusiveLock();
        check = false;

        Thread thread1 = new Thread(new Runnable() {

            @Override
            public void run() {
                LockingUnit.instance.lock("test", LockType.Exclusive.LOCK);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    fail();
                }
                check = true;
                LockingUnit.instance.release("test", LockType.Exclusive.LOCK);
            }
        });

        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                LockingUnit.instance.lock("test", LockType.Exclusive.LOCK);
                assertEquals("Thread1 did not block Thread2", check, true);
                LockingUnit.instance.release("test", LockType.Exclusive.LOCK);
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
                LockingUnit.instance.lock("test", LockType.Default.READ_LOCK);
                LockingUnit.instance.lock("test11", LockType.Default.READ_LOCK);
                try {
                    if (!sem.tryAcquire(1, 1000, TimeUnit.MILLISECONDS))
                        fail("Thread2 did not release the semaphore and was blocked on the read lock");
                } catch (InterruptedException e) {
                    fail();
                }
                LockingUnit.instance.release("test11", LockType.Default.READ_LOCK);
                LockingUnit.instance.release("test", LockType.Default.READ_LOCK);
            }
        });

        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                LockingUnit.instance.lock("test22", LockType.Default.READ_LOCK);
                LockingUnit.instance.lock("test", LockType.Default.READ_LOCK);
                sem.release();
                LockingUnit.instance.release("test", LockType.Default.READ_LOCK);
                LockingUnit.instance.release("test22", LockType.Default.READ_LOCK);
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
                LockingUnit.instance.lock("test", LockType.Default.WRITE_LOCK);
                try {
                    if (sem.tryAcquire(1, 1000, TimeUnit.MILLISECONDS))
                        fail("Thread2 could release the semaphore and was not blocked on the write lock");
                } catch (InterruptedException e) {
                    fail();
                }
                LockingUnit.instance.release("test", LockType.Default.WRITE_LOCK);
            }
        });

        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                LockingUnit.instance.lock("test", LockType.Default.WRITE_LOCK);
                sem.release();
                LockingUnit.instance.release("test", LockType.Default.WRITE_LOCK);
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
                LockingUnit.instance.lock("test", LockType.Default.READ_LOCK);
                LockingUnit.instance.promote("test", asList(LockType.Default.READ_LOCK), LockType.Default.WRITE_LOCK);
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
                LockingUnit.instance.release("test", LockType.Default.WRITE_LOCK);
            }
        });

        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                LockingUnit.instance.lock("test", LockType.Default.READ_LOCK);
                sem.release();
                LockingUnit.instance.release("test", LockType.Default.READ_LOCK);
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
                LockingUnit.instance.lock("test", LockType.Default.READ_LOCK);
                try {
                    t2AcquiredTheLockSem.acquire();
                    LockingUnit.instance.promote("test", asList(LockType.Default.READ_LOCK), LockType.Default.WRITE_LOCK);

                    if (!t2ReleasedTheLockSem.tryAcquire()) {
                        fail("Thread1 could promote while t2 had the lock.");
                    }
                } catch (InterruptedException e1) {
                    fail();
                }
                LockingUnit.instance.release("test", LockType.Default.WRITE_LOCK);
            }
        });

        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    LockingUnit.instance.lock("test", LockType.Default.READ_LOCK);
                    t2AcquiredTheLockSem.release();

                    Thread.sleep(1500);

                    LockingUnit.instance.release("test", LockType.Default.READ_LOCK);
                    t2ReleasedTheLockSem.release();
                } catch (InterruptedException e) {
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
