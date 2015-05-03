package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.ScheduledTestCase.ScheduledCommand;

import org.junit.Before;
import org.junit.Test;


public class MVCC2PLTest extends AlgorithmScheduledTest {

    @Before
    public void setUp() {
        instance = new Simple2PL();
    }

    @Test
    public void testDeadlock() {
        ScheduledCommand[][] schedule = {
        /* T1 */{ BEGIN(), R("x", null, t), _______________, W("y", "y1", t), _______________, COMM(t), _______ },
        /* T2 */{ BEGIN(), _______________, R("y", null, t), _______________, W("x", "x2", f), _______, COMM(f) } };
        new ScheduleExecutor(schedule).execute();
    }
    @Test
    public void testComplex(){
        ScheduledCommand[][] schedule = {
            /* T1 */{BEGIN(),R("x", null, t),_______,_______________,R(null, null, t)},
            /* T2 */{_______,_______________,BEGIN(),W("y", "y2", t)},
            /* T3 */{_______,_______________,_______,_______________},
            /* T4 */{_______,_______________,_______,_______________}
        };
//        new ScheduleExecutor(schedule).execute();
        }
    }
            
}
