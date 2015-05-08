package ch.epfl.tkvs.transactionmanager.algorithms;

import org.junit.Before;
import org.junit.Test;

import ch.epfl.tkvs.ScheduledTestCase;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.PrepareRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;


public class AlgorithmScheduledTest extends ScheduledTestCase {

    CCAlgorithm instance;
    final boolean t = true;
    final boolean f = false;
    final ScheduledCommand _______________ = _______;

    @Before
    public void setUp() {
        instance = new MVCC2PL(null, null);
        System.out.println("\nNew Test");
    }

    public ScheduledCommand BEGIN() {
        return new ScheduledCommand() {

            @Override
            public void perform(int tid, int step) {

                System.out.println("Begin " + tid);
                GenericSuccessResponse gsr = instance.begin(new BeginRequest(tid));
                assertEquals(gsr.getSuccess(), true);
            }
        };
    }

    public ScheduledCommand R(final String key, final String expected, final boolean shouldSucceed) {
        return new ScheduledBlockingCommand() {

            @Override
            public void perform(int tid, int step) {

                System.out.println("Read " + tid + " " + key);
                ReadResponse rr = instance.read(new ReadRequest(tid, key, 0));

                if (shouldSucceed) {
                    assertEquals(true, rr.getSuccess());
                    assertEquals(expected, (String) rr.getValue());
                } else
                    assertEquals(false, rr.getSuccess());

            }
        };
    }

    public ScheduledCommand W(final String key, final String value, final boolean shouldSucceed) {
        return new ScheduledBlockingCommand() {

            @Override
            public void perform(int tid, int step) {

                System.out.println("Write " + tid + " " + key);
                GenericSuccessResponse gsr = instance.write(new WriteRequest(tid, key, value, 0));
                assertEquals(shouldSucceed, gsr.getSuccess());

            }
        };
    }

    public ScheduledCommand COMM(final boolean shouldSucceed) {
        return new ScheduledBlockingCommand() {

            @Override
            public void perform(int tid, int step) {

                System.out.println("Commit " + tid);
                GenericSuccessResponse gsr = instance.prepare(new PrepareRequest(tid));
                assertEquals(shouldSucceed, gsr.getSuccess());
                gsr = instance.commit(new CommitRequest(tid));
                assertEquals(shouldSucceed, gsr.getSuccess());

            }
        };
    }

    @Test
    public void testRead() {

        ReadRequest request = new ReadRequest(0, 0, 0);

        ReadResponse result = instance.read(request);
        assertEquals(false, result.getSuccess());

        GenericSuccessResponse gsr;
        gsr = instance.begin(new BeginRequest(0));
        assertEquals(true, gsr.getSuccess());

        result = instance.read(request);
        assertEquals(false, result.getSuccess());

        gsr = instance.begin(new BeginRequest(0));
        assertEquals(true, gsr.getSuccess());

        gsr = instance.write(new WriteRequest(0, 0, "init", 0));
        assertEquals(true, gsr.getSuccess());

        result = instance.read(request);
        assertEquals(true, result.getSuccess());
        assertEquals("init", result.getValue());

    }

    @Test
    public void testWrite() {

        WriteRequest request = new WriteRequest(0, 0, 0, 0);

        GenericSuccessResponse result = instance.write(request);
        assertEquals(false, result.getSuccess());

        GenericSuccessResponse br;
        br = instance.begin(new BeginRequest(0));
        assertEquals(true, br.getSuccess());

        result = instance.write(request);
        assertEquals(true, result.getSuccess());

    }

    @Test
    public void testBegin() {

        BeginRequest request = new BeginRequest(0);

        GenericSuccessResponse result = instance.begin(request);
        assertEquals(true, result.getSuccess());

        result = instance.begin(request);
        assertEquals(false, result.getSuccess());
    }

    /**
     * Test of commit method, of class MVCC2PL.
     */
    @Test
    public void testCommit() {

        CommitRequest request = new CommitRequest(0);

        GenericSuccessResponse result = instance.commit(request);
        assertEquals(false, result.getSuccess());

        GenericSuccessResponse br;
        br = instance.begin(new BeginRequest(0));
        assertEquals(true, br.getSuccess());

        result = instance.commit(request);
        assertEquals(false, result.getSuccess());

        result = instance.prepare(new PrepareRequest(0));
        assertEquals(true, result.getSuccess());

        result = instance.commit(request);
        assertEquals(true, result.getSuccess());

        result = instance.commit(request);
        assertEquals(false, result.getSuccess());

        ReadResponse rr = instance.read(new ReadRequest(0, 0, 0));
        assertEquals(false, rr.getSuccess());

        result = instance.write(new WriteRequest(0, 0, 0, 0));
        assertEquals(false, result.getSuccess());

        // WHAT SHOULD BE RESULT, true or false? At the moment it will
        // succeed and test will fail
        result = instance.begin(new BeginRequest(0));
        assertEquals(true, result.getSuccess());

    }

    @Test
    public void testSingle() {

        GenericSuccessResponse br = instance.begin(new BeginRequest(1));
        assertEquals(true, br.getSuccess());

        GenericSuccessResponse wr = instance.write(new WriteRequest(1, 0, "zero", 0));
        assertEquals(true, wr.getSuccess());

        ReadResponse rr = instance.read(new ReadRequest(1, 0, 0));
        assertEquals(true, rr.getSuccess());
        assertEquals("zero", (String) rr.getValue());

        GenericSuccessResponse pr = instance.prepare(new PrepareRequest(1));
        assertEquals(true, pr.getSuccess());

        GenericSuccessResponse cr = instance.commit(new CommitRequest(1));
        assertEquals(true, cr.getSuccess());

    }

    @Test
    public void testSerial() {

        GenericSuccessResponse gsr = instance.begin(new BeginRequest(0));
        assertEquals(true, gsr.getSuccess());

        gsr = instance.write(new WriteRequest(0, 0, "zero", 0));
        assertEquals(true, gsr.getSuccess());

        gsr = instance.write(new WriteRequest(0, 1, "ONE", 0));
        assertEquals(true, gsr.getSuccess());

        gsr = instance.write(new WriteRequest(0, 1, "one", 0));
        assertEquals(true, gsr.getSuccess());

        gsr = instance.prepare(new PrepareRequest(0));
        assertEquals(true, gsr.getSuccess());

        gsr = instance.commit(new CommitRequest(0));
        assertEquals(true, gsr.getSuccess());

        gsr = instance.begin(new BeginRequest(1));
        assertEquals(true, gsr.getSuccess());

        ReadResponse rr = instance.read(new ReadRequest(1, 1, 0));
        assertEquals(true, rr.getSuccess());
        assertEquals("one", (String) rr.getValue());

        rr = instance.read(new ReadRequest(1, 0, 0));
        assertEquals(true, rr.getSuccess());
        assertEquals("zero", (String) rr.getValue());

        gsr = instance.prepare(new PrepareRequest(1));
        assertEquals(true, gsr.getSuccess());

        gsr = instance.commit(new CommitRequest(1));
        assertEquals(true, gsr.getSuccess());

    }

    public void initializeKeys(String... keys) {
        ScheduledCommand[][] init = new ScheduledCommand[1][];
        init[0] = new ScheduledCommand[keys.length + 2];
        int i = 0;
        init[0][i++] = BEGIN();

        for (String key : keys) {
            init[0][i++] = W(key, key + "0", t);

        }
        init[0][i++] = COMM(t);
        new ScheduleExecutor(init).execute();
    }

    @Test
    public void testScheduledSerial() {
        ScheduledCommand[][] schedule = {
        /* T1 */{ BEGIN(), W("x", "x1", t), W("y", "y1", t), W("y", "y2", t), COMM(true), _______, _______________, _______________, _______ },
        /* T2 */{ _______, _______________, _______________, _______________, _______, BEGIN(), R("x", "x1", t), R("y", "y2", t), COMM(true) } };
        new ScheduleExecutor(schedule).execute();
    }

}
