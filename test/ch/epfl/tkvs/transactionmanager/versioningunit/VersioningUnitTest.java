package ch.epfl.tkvs.transactionmanager.versioningunit;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import ch.epfl.tkvs.keyvaluestore.KeyValueStore;


public class VersioningUnitTest extends TestCase {

    private VersioningUnit V = VersioningUnit.instance;

    @Before
    public void setUp() throws Exception {
        V.init();

        KeyValueStore.instance.clear();
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
                V.commit(xid1);

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
                V.commit(xid2);
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
                V.commit(xid3);
            }
        });

        V.put(xid4, "key0", "value00");
        V.put(xid4, "key1", "value01");
        V.put(xid4, "key2", "value02");
        V.put(xid4, "key3", "value03");
        V.commit(xid4);

        // V.commit(xid5);
        assertTrue(V.get(xid5, "key0").equals("value00"));
        assertTrue(V.get(xid5, "key1").equals("value01"));
        assertTrue(V.get(xid5, "key2").equals("value02"));
        assertTrue(V.get(xid5, "key3").equals("value03"));

        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();

    }

}
