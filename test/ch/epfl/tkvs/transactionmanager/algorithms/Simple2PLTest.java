package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.ScheduledTestCase;
import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.PrepareRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import org.junit.Before;
import org.junit.Test;


public class Simple2PLTest extends AlgorithmScheduledTest {

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
}
