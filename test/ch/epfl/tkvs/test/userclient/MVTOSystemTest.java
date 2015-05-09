package ch.epfl.tkvs.test.userclient;

import org.junit.Test;


public class MVTOSystemTest extends UserClientScheduledTest {

    @Test
    public void testComplex() {
        MyKey x = freshKey("x", 0);
        MyKey y = freshKey("y", 1);
        MyKey z = freshKey("z", 2);
        TransactionExecutionCommand schedule[][] = {
        /* T1 */{ BEGIN__(0, t), READ(x, "00"), _____________, _____________, _____________, _____________, _____________, _____________, _____________, _____________, _____________, READ(y, "00"), COMMIT____(t) },
        /* T2 */{ _____________, BEGIN__(1, t), READ(x, "00"), W(x, "x2", t), _____________, _____________, READ(y, "00"), W(y, "y2", t), COMMIT____(t) },
        /* T3 */{ _____________, _____________, _____________, BEGIN__(2, t), READ(x, "x2"), _____________, READ(z, "00"), COMMIT____(t) },
        /* T4 */{ _____________, _____________, _____________, _____________, BEGIN__(0, t), READ(x, "x2"), W(x, "x4", t), _____________, READ(y, "y2"), W(y, "y4", f) },
        /* T5 */{ _____________, _____________, _____________, _____________, _____________, _____________, _____________, BEGIN__(1, t), READ(y, "y2"), READ(z, "00"), COMMIT____(t) },
        /* T6 */{ _____________, _____________, _____________, _____________, _____________, _____________, _____________, _____________, _____________, _____________, BEGIN__(1, t), READ(y, "y2"), COMMIT____(t) } };
        execute(schedule, x, y, z);
    }
}
