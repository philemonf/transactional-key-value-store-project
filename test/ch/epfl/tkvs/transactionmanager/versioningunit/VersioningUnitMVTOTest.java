package ch.epfl.tkvs.transactionmanager.versioningunit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.epfl.tkvs.ScheduledTestCase;
import ch.epfl.tkvs.user.AbortException;


public class VersioningUnitMVTOTest extends ScheduledTestCase {
    
    private VersioningUnitMVTO V = VersioningUnitMVTO.instance;

    @Before
    public void setUp() throws Exception {
        V.init();
    }

    @After
    public void tearDown() throws Exception {
    }
    
    public ScheduledCommand B(final int xid) {
        return new ScheduledCommand() {
            public void perform() {
                V.begin_transaction(xid);
            }
        };
    }
    
    public ScheduledCommand R(final int xid, final int key) {
        return new ScheduledCommand() {
            public void perform() {
                V.get(xid, key);
            }
        };
    }

    public ScheduledCommand W(final int xid, final int key, final int value) {
        return new ScheduledCommand() {
            @Override
            public void perform() {
                try {
                    V.put(xid, key, value);
                } catch (AbortException e) {
                }
            }
        };
    }
    
    public ScheduledCommand C(final int xid) {
        return new ScheduledCommand() {
            public void perform() {
                try {
                    V.commit(xid);
                } catch (AbortException e) {
                }
            }
        };
    }
    
    public ScheduledBlockingCommand CB(final int xid) {
        return new ScheduledBlockingCommand() {
            public void perform() {
                try {
                    V.commit(xid);
                } catch (AbortException e) {
                }
            }
        };
    }
    
    @Test
    public void test1() {        
        ScheduledCommand[][] schedule = {
            /* T1: */ { B(1),  ___,      ___, W(1,1,1),    C(1),    ___,      ___,    ___,    ___,  ___,  ___,    ___,    ___,  ___ },
            /* T2: */ {  ___, B(2),      ___,      ___,     ___, R(2,1),      ___, R(2,1),    ___, C(2),  ___,    ___,    ___,  ___ },
            /* T3: */ {  ___,  ___,     B(3),      ___,     ___,    ___, W(3,1,5),    ___, R(3,1),  ___, C(3),    ___,    ___,  ___ },
            /* T4: */ {  ___,  ___,      ___,      ___,     ___,    ___,      ___,    ___,    ___,  ___,  ___,   B(4), R(4,1), C(4) }
        };
        ScheduleExecutor executor = new ScheduleExecutor(schedule);
        executor.execute();
    }
    
    @Test
    public void test2() {
        
        ScheduledCommand[][] schedule = {
            /* T1: */ { B(1),  ___, W(1,1,1),    ___,   ___,      ___,  C(1)  },
            /* T2: */ {  ___, B(2),      ___, R(2,1), CB(2),  Wt(2,4),   ___  }
        };
        ScheduleExecutor executor = new ScheduleExecutor(schedule);
        executor.execute();
    }
}
