package ch.epfl.tkvs.transactionmanager.lockingunit;

import junit.framework.TestCase;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class LockingUnitTest extends TestCase {

    public static enum TestSimpleLockType {
        THE_LOCK
    }

    public static boolean check = true;

    public void testSimpleLock() throws Exception {
         LockingUnit.instance.initWithLockCompatibilityTable(new LockCompatibilityTable() {
             @Override
             public <E extends Enum<E>> boolean areCompatible(E lock1, E lock2) {
                 return false; // if there is a lock there is no compatibility.
             }
         });

        check = true;

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                LockingUnit.instance.lock("test", TestSimpleLockType.THE_LOCK);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    fail();
                }
                check = false;
                LockingUnit.instance.release("test", TestSimpleLockType.THE_LOCK);
            }
        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                LockingUnit.instance.lock("test", TestSimpleLockType.THE_LOCK);
                assertEquals(check, true);
                LockingUnit.instance.release("test", TestSimpleLockType.THE_LOCK);
            }
        });

        thread1.run();
        Thread.sleep(2000);
        thread2.run();

        thread1.join();
        thread2.join();
        assertEquals(check, false);
    }

    public void testDefaultLockShared() throws Exception {
        // Init with default parameters
        LockingUnit.instance.initWithLockCompatibilityTable(null);

        final Semaphore sem = new Semaphore(0);
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                LockingUnit.instance.lock("test", LockingUnit.DefaultLockType.READ_LOCK);
                try {
                    if (!sem.tryAcquire(1, 5000, TimeUnit.MILLISECONDS)) {
                        fail();
                    }
                } catch (InterruptedException e) {
                    fail();
                }
                LockingUnit.instance.release("test", LockingUnit.DefaultLockType.READ_LOCK);
            }
        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                LockingUnit.instance.lock("test", LockingUnit.DefaultLockType.READ_LOCK);
                sem.release();
                LockingUnit.instance.release("test", LockingUnit.DefaultLockType.READ_LOCK);
            }
        });

        thread1.run();
        Thread.sleep(2000);
        thread2.run();
    }

    public void testDefaultLockWrite() throws Exception {
        // Init with default parameters
        LockingUnit.instance.initWithLockCompatibilityTable(null);

        final Semaphore sem = new Semaphore(0);
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                LockingUnit.instance.lock("test", LockingUnit.DefaultLockType.WRITE_LOCK);
                try {
                    if (sem.tryAcquire(1, 5000, TimeUnit.MILLISECONDS)) {
                        fail();
                    }
                } catch (InterruptedException e) {
                    fail();
                }
                LockingUnit.instance.release("test", LockingUnit.DefaultLockType.WRITE_LOCK);
            }
        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                LockingUnit.instance.lock("test", LockingUnit.DefaultLockType.WRITE_LOCK);
                sem.release();
                LockingUnit.instance.release("test", LockingUnit.DefaultLockType.WRITE_LOCK);
            }
        });

        thread1.run();
        Thread.sleep(2000);
        thread2.run();
    }

}
