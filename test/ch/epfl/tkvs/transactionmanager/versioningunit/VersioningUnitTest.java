package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockType;
import ch.epfl.tkvs.transactionmanager.lockingunit.LockingUnit;


public class VersioningUnitTest extends TestCase {

    private VersioningUnit V = VersioningUnit.getInstance();

    @Before
    public void setUp() throws Exception {
        V.init();
        KeyValueStore.instance.clear();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        V.stopNow();
    }

    @Test
    public void testSingleXact() {
        final int xid = 1;
        V.put(xid, "key1", "value1");
        assertTrue(V.get(xid, "key1").equals("value1"));
        V.commit(xid);
        assertTrue(V.get(xid, "key1").equals("value1"));
    }

    @Test
    public void testAbort() {
        final int xid = 1;
        V.put(xid, "key1", "value1");
        V.abort(xid);
        assertTrue(V.get(xid, "key1") == null);
    }

    @Test
    public void testTwoXact() {
        final int xid1 = 1, xid2 = 2;
        V.put(xid1, "key", "value1");
        V.put(xid2, "key", "value2");
        assertTrue(V.get(xid1, "key").equals("value1"));
        assertTrue(V.get(xid2, "key").equals("value2"));
    }

    @Test
    public void testCommit() {
        final int xid1 = 1, xid2 = 2, xid3 = 3;
        V.put(xid1, "key", "value1");
        V.put(xid3, "key", "value3");
        V.commit(xid1);
        assertTrue(V.get(xid2, "key").equals("value1"));
        V.commit(xid3);
        assertTrue(V.get(xid2, "key").equals("value3"));

    }

    @Test
    public void testCommitEmpty() {
        final int xid1 = 1, xid2 = 2, xid3 = 3;

        V.put(xid1, "key1", "value11");
        V.put(xid1, "key2", "value12");
        V.commit(xid1);
        V.commit(xid2);
        // Not possible in our application
        // V.put(xid2, "key1", "value21");
        // V.put(xid2, "key2", "value22");

        assertTrue(V.get(xid3, "key1").equals("value11"));
        assertTrue(V.get(xid3, "key2").equals("value12"));
    }

    @Test
    public void testReadWrite() throws InterruptedException {
        final int xid1 = 1, xid2 = 2, xid3 = 3, xid4 = 4;
        final Thread t1 = new Thread(new Runnable() {

            @Override
            public void run() {
                V.put(xid1, "key", "value1");
                V.put(xid3, "key1", "value3");
                V.commit(xid1);
            }
        });

        final Thread t2 = new Thread(new Runnable() {

            @Override
            public void run() {
                V.put(xid4, "key1", "value4");
                try {
                    t1.join();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                V.commit(xid4);
            }
        });

        t1.start();
        t2.start();
        t2.join();

        assertTrue(V.get(xid2, "key").equals("value1"));
        assertTrue(V.get(xid2, "key1").equals("value4"));

    }

    @Test
    public void testWriteRead() throws InterruptedException {

        V.init();

        final int xid1 = 1, xid2 = 2, xid3 = 3, xid4 = 4;
        final Thread t1 = new Thread(new Runnable() {

            @Override
            public void run() {
                V.put(xid1, "key", "value1");
                V.commit(xid1);
                assertTrue(V.get(xid2, "key").equals("value1"));
                V.put(xid4, "key", "value4");
                V.commit(xid4);

            }
        });

        final Thread t2 = new Thread(new Runnable() {

            @Override
            public void run() {
                V.put(xid3, "key", "value3");
                try {
                    t1.join();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                assertTrue(V.get(xid2, "key").equals("value4"));
                V.commit(xid3);
            }
        });

        t1.start();
        t2.start();
        t2.join();
        assertTrue(V.get(xid2, "key").equals("value3"));

    }

    @Test
    public void testWriteReadTread3() throws InterruptedException {
        final int xid1 = 1, xid2 = 2, xid3 = 3, xid4 = 4, xid5 = 5;

        final Thread t1 = new Thread(new Runnable() {

            @Override
            public void run() {
                V.put(xid1, "key0", "value10");
                V.put(xid1, "key1", "value11");
                V.put(xid1, "key2", "value12");
                V.put(xid1, "key3", "value13");
                V.put(xid1, "key4", "value14");

            }
        });

        Thread t2 = new Thread(new Runnable() {

            @Override
            public void run() {
                V.put(xid2, "key0", "value20");
                V.put(xid2, "key1", "value21");
                V.put(xid2, "key2", "value22");
                V.put(xid2, "key3", "value23");
                V.put(xid2, "key4", "value24");
            }
        });

        Thread t3 = new Thread(new Runnable() {

            @Override
            public void run() {
                V.put(xid3, "key0", "value30");
                V.put(xid3, "key1", "value31");
                V.put(xid3, "key2", "value32");
                V.put(xid3, "key3", "value33");
                V.put(xid3, "key4", "value34");
            }
        });

        t1.start();
        t2.start();
        t3.start();

        V.put(xid4, "key0", "value00");
        V.put(xid4, "key1", "value01");
        V.put(xid4, "key2", "value02");
        V.put(xid4, "key3", "value03");
        V.commit(xid4);

        int i = 0;
        assertTrue(V.get(xid5, "key0").equals("value" + i + "0"));
        assertTrue(V.get(xid5, "key1").equals("value" + i + "1"));
        assertTrue(V.get(xid5, "key2").equals("value" + i + "2"));
        assertTrue(V.get(xid5, "key3").equals("value" + i + "3"));

        V.commit(xid1);
        i = 1;
        assertTrue(V.get(xid5, "key0").equals("value" + i + "0"));
        assertTrue(V.get(xid5, "key1").equals("value" + i + "1"));
        assertTrue(V.get(xid5, "key2").equals("value" + i + "2"));
        assertTrue(V.get(xid5, "key3").equals("value" + i + "3"));

        V.commit(xid2);
        i = 2;
        assertTrue(V.get(xid5, "key0").equals("value" + i + "0"));
        assertTrue(V.get(xid5, "key1").equals("value" + i + "1"));
        assertTrue(V.get(xid5, "key2").equals("value" + i + "2"));
        assertTrue(V.get(xid5, "key3").equals("value" + i + "3"));

        V.commit(xid3);
        i = 3;
        assertTrue(V.get(xid5, "key0").equals("value" + i + "0"));
        assertTrue(V.get(xid5, "key1").equals("value" + i + "1"));
        assertTrue(V.get(xid5, "key2").equals("value" + i + "2"));
        assertTrue(V.get(xid5, "key3").equals("value" + i + "3"));

        t1.join();
        t2.join();
        t3.join();

    }

    @Test
    public void testWriteReadSemaphore() throws InterruptedException {
        final int xid1 = 1, xid2 = 2, xid3 = 3, xid4 = 4, xid5 = 5, xid6 = 6, xid7 = 7, xid8 = 8;

        final Semaphore sem1 = new Semaphore(0);
        final Semaphore sem2 = new Semaphore(0);
        final Semaphore sem3 = new Semaphore(0);
        final Semaphore sem4 = new Semaphore(0);

        final Thread t1 = new Thread(new Runnable() {

            public void run() {
                V.put(xid1, "key0", "value00");
                V.put(xid1, "key1", "value01");

                try {
                    sem1.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                V.commit(xid1);

            }
        });

        final Thread t2 = new Thread(new Runnable() {

            public void run() {
                V.put(xid2, "key0", "value20");
                V.put(xid2, "key1", "value21");

                try {
                    sem2.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                V.commit(xid2);

            }
        });

        final Thread t3 = new Thread(new Runnable() {

            public void run() {
                V.put(xid3, "key0", "value30");
                V.put(xid3, "key1", "value31");

                try {
                    sem3.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });

        final Thread t4 = new Thread(new Runnable() {

            public void run() {
                V.put(xid4, "key0", "value40");
                V.put(xid4, "key1", "value41");

                try {
                    sem4.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                V.commit(xid4);
            }
        });

        t1.start();
        t2.start();
        t3.start();
        t4.start();

        sem1.release();
        t1.join();
        // T1 done
        assertTrue(V.get(xid5, "key0").equals("value00"));
        assertTrue(V.get(xid5, "key1").equals("value01"));

        sem4.release();
        t4.join();
        assertTrue(V.get(xid8, "key0").equals("value40"));
        assertTrue(V.get(xid8, "key1").equals("value41"));

        sem3.release();
        t3.join();
        assertTrue(V.get(xid7, "key0").equals("value40"));
        assertTrue(V.get(xid7, "key1").equals("value41"));

        sem2.release();
        t2.join();
        assertTrue(V.get(xid6, "key0").equals("value20"));
        assertTrue(V.get(xid6, "key1").equals("value21"));

    }

}
