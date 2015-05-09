package ch.epfl.tkvs.test.userclient;

import org.junit.Test;


public class S2PLSystemTest extends UserClientScheduledTest {

    @Test
    public void testDeadlock() {

        MyKey x = freshKey("x", 0);
        MyKey y = freshKey("y", 1);
        TransactionExecutionCommand schedule[][] = {
        /* T1 */{ BEGIN__(0, t), READ(x, "00"), W(y, "y1", t), COMMIT____(t) },
        /* T2 */{ BEGIN__(1, t), READ(y, "00"), W(x, "x2", t), COMMIT____(t) } };
        execute(schedule, x, y);

    }
}
