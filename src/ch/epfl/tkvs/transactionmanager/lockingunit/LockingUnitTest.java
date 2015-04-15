package ch.epfl.tkvs.transactionmanager.lockingunit;

import junit.framework.TestCase;

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
        assertEquals(check, false);
    }

}
