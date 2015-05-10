package ch.epfl.tkvs.test.userclient;

import org.junit.Test;


public class MV2PLSystemTest extends UserClientScheduledTest {

    @Test
    public void testComplex() {

        MyKey x = freshKey("x", 0);
        MyKey y = freshKey("y", 1);
        MyKey z = freshKey("z", 2);
        TransactionExecutionCommand schedule[][] = {
        /* T1 */{ BEGIN__(0, t), READ(x, "00"), _____________, _____________, READ(y, "00"), W(x, "x1", t), COMMIT____(t) },
        /* T2 */{ _____________, _____________, BEGIN__(1, t), W(y, "y2", t), _____________, _____________, _____________, _____________, _____________, _____________, W(x, "x2", t), COMMIT____(t) },
        /* T3 */{ _____________, _____________, _____________, _____________, _____________, _____________, BEGIN__(2, t), READ(y, "00"), READ(z, "00"), W(z, "z3", t), _____________, _____________, _____________, _____________, COMMIT____(t) },
        /* T4 */{ _____________, _____________, _____________, _____________, _____________, _____________, _____________, _____________, _____________, _____________, _____________, BEGIN__(2, t), W(z, "z4", t), COMMIT____(t), } };
        execute(schedule, x, y, z);

    }
}
