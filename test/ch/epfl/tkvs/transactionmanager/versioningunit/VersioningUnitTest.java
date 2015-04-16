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
        V.kvStore = new KeyValueStore();
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
}
