package ch.epfl.tkvs.transactionmanager.algorithms;

import org.junit.Before;
import org.junit.Test;


public class Simple2PLTest extends AlgorithmScheduledTest {

    @Before
    public void setUp() {
        instance = new Simple2PL(null, null);
        System.out.println("\nNew Test");
    }

    @Test
    public void testDeadlock() {
        ScheduledCommand[][] schedule = {
        /* T1 */{ BEGIN(), R("x", "x0", t), _______________, W("y", "y1", t), _______________, COMM(t), _______ },
        /* T2 */{ BEGIN(), _______________, R("y", "y0", t), _______________, W("x", "x2", f), _______, COMM(f) } };
        initializeKeys("x", "y");
        new ScheduleExecutor(schedule).execute();
    }
}
