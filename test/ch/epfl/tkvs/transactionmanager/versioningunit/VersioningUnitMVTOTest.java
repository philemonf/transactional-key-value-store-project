package ch.epfl.tkvs.transactionmanager.versioningunit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.epfl.tkvs.ScheduledTestCase;
import ch.epfl.tkvs.transactionmanager.AbortException;


public class VersioningUnitMVTOTest extends ScheduledTestCase {

    private VersioningUnitMVTO V = VersioningUnitMVTO.getInstance();

    @Before
    public void setUp() throws Exception {
        V.init();
    }

    @After
    public void tearDown() throws Exception {
    }

    public ScheduledCommand BEGIN() {
        return new ScheduledCommand() {

            public void perform(int tid, int step) {
                V.begin_transaction(tid);
            }
        };
    }

    public ScheduledCommand R(final int key, final int expected) {
        return new ScheduledCommand() {

            public void perform(int tid, int step) {
                assertEquals(expected, V.get(tid, key));
            }
        };
    }

    public ScheduledCommand W(final int key, final int value) {
        return new ScheduledCommand() {

            @Override
            public void perform(int tid, int step) {
                try {
                    V.put(tid, key, value);
                } catch (AbortException e) {
                }
            }
        };
    }

    public ScheduledCommand CMMIT() {
        return new ScheduledCommand() {

            public void perform(int tid, int step) {
                try {
                    V.prepareCommit(tid);
                    V.commit(tid);
                } catch (AbortException e) {
                }
            }
        };
    }

    public ScheduledBlockingCommand CMMTB() {
        return new ScheduledBlockingCommand() {

            public void perform(int tid, int step) {
                try {
                    V.prepareCommit(tid);
                    V.commit(tid);
                } catch (AbortException e) {
                }
            }
        };
    }

    @Test
    public void test1() {
        ScheduledCommand[][] schedule = {
        /* T1: */{ BEGIN(), _______, _______, W(1, 1), CMMIT(), _______, _______, _______, _______, _______, _______, _______, _______, _______ },
        /* T2: */{ _______, BEGIN(), _______, _______, _______, R(1, 1), _______, R(1, 1), _______, CMMIT(), _______, _______, _______, _______ },
        /* T3: */{ _______, _______, BEGIN(), _______, _______, _______, W(1, 5), _______, R(1, 5), _______, CMMIT(), _______, _______, _______ },
        /* T4: */{ _______, _______, _______, _______, _______, _______, _______, _______, _______, _______, _______, BEGIN(), R(1, 5), CMMIT() } };
        ScheduleExecutor executor = new ScheduleExecutor(schedule);
        executor.execute();
    }

    @Test
    public void test2() {

        ScheduledCommand[][] schedule = {
        /* T1: */{ BEGIN(), _______, W(1, 1), _______, _______, _______, CMMIT() },
        /* T2: */{ _______, BEGIN(), _______, R(1, 1), CMMTB(), Wait(4), _______ } };
        ScheduleExecutor executor = new ScheduleExecutor(schedule);
        executor.execute();
    }

    @Test
    public void test3() {

        ScheduledCommand[][] schedule = {
        /* T1: */{ BEGIN(), W(1, 1), CMMIT(), _______, _______, _______, _______, _______, _______, _______, _______, _______, _______ },
        /* T2: */{ _______, _______, _______, BEGIN(), R(1, 1), _______, _______, W(1, 2), CMMIT(), _______, _______, _______, _______ },
        /* T3: */{ _______, _______, _______, _______, _______, BEGIN(), R(1, 1), _______, _______, CMMIT(), _______, _______, _______ },
        /* T4: */{ _______, _______, _______, _______, _______, _______, _______, _______, _______, _______, BEGIN(), R(1, 1), CMMIT() } };
        ScheduleExecutor executor = new ScheduleExecutor(schedule);
        executor.execute();
    }
}
