package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.ScheduledTestCase.ScheduledCommand;

import org.junit.Before;
import org.junit.Test;


public class MVCC2PLTest extends AlgorithmScheduledTest {

    @Before
    public void setUp() {
        instance = new MVCC2PL(null);
        System.out.println("\nNew Test");
    }

    @Test
    public void testDeadlock() {
        ScheduledCommand[][] schedule = {
        /* T1 */{ BEGIN(), R("x", null, t), _______________, W("y", "y1", t), _______________, COMM(t), _______ },
        /* T2 */{ BEGIN(), _______________, R("y", null, t), _______________, W("x", "x2", t), _______, COMM(f) } };
        new ScheduleExecutor(schedule).execute();
    }

    @Test
    public void testComplex() {
        ScheduledCommand[][] schedule = {
        /* T1 */{ BEGIN(), R("x", null, t), _______, _______________, R("y", null, t), W("x", "x1", t), COMM(t), _______________, _______________, _______________, _______________, _______, _______________, _______, _______ },
        /* T2 */{ _______, _______________, BEGIN(), W("y", "y2", t), _______________, _______________, _______, _______________, _______________, _______________, W("x", "x2", t), COMM(t), _______________, _______, _______ },
        /* T3 */{ _______, _______________, _______, _______________, _______________, _______________, BEGIN(), R("y", null, t), R("z", null, t), W("z", "z3", t), _______________, _______, _______________, _______, COMM(t) },
        /* T4 */{ _______, _______________, _______, _______________, _______________, _______________, _______, _______________, _______________, _______________, _______________, BEGIN(), W("z", "z4", t), COMM(t), _______ } };
        new ScheduleExecutor(schedule).execute();
    }
}
