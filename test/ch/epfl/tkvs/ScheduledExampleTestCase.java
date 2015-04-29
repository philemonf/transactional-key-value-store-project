package ch.epfl.tkvs;

import java.util.concurrent.Semaphore;

import org.junit.Test;


/**
 * Created with IntelliJ IDEA. User: philemonfavrod Date: 27/04/15 Time: 21:06 To change this template use File |
 * Settings | File Templates.
 */
public class ScheduledExampleTestCase extends ScheduledTestCase {

    final Semaphore sem = new Semaphore(1);

    public ScheduledBlockingCommand L() {
        return new ScheduledBlockingCommand() {

            @Override
            public void perform() {
                try {
                    sem.acquire();
                } catch (InterruptedException e) {
                }
            }
        };
    }

    public ScheduledCommand U() {
        return new ScheduledCommand() {

            @Override
            public void perform() {
                sem.release();
            }
        };
    }

    @Test
    public void test() {
        ScheduledCommand[][] schedule = { { L(), ___, ___, U(), ___ }, { ___, L(), Wt(1, 1), ___, U() } };
        ScheduleExecutor executor = new ScheduleExecutor(schedule);
        executor.execute();
    }
}
