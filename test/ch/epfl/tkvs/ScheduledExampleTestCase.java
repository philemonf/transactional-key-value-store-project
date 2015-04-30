package ch.epfl.tkvs;

import java.util.concurrent.Semaphore;

import org.junit.Test;


/**
 * Created with IntelliJ IDEA. User: philemonfavrod Date: 27/04/15 Time: 21:06 To change this template use File |
 * Settings | File Templates.
 */
public class ScheduledExampleTestCase extends ScheduledTestCase {

    final Semaphore sem = new Semaphore(1);

    public ScheduledBlockingCommand LockS() {
        return new ScheduledBlockingCommand() {

            @Override
            public void perform(int tid, int step) {
                try {
                    sem.acquire();
                } catch (InterruptedException e) {
                }
            }
        };
    }

    public ScheduledCommand ULock() {
        return new ScheduledCommand() {

            @Override
            public void perform(int tid, int step) {
                sem.release();
            }
        };
    }

    @Test
    public void test() {
        ScheduledCommand[][] schedule = {
        /* T1: */{ LockS(), _______, _______, ULock(), _______ },
        /* T2: */{ _______, LockS(), Wait(1), _______, ULock() } };
        ScheduleExecutor executor = new ScheduleExecutor(schedule);
        executor.execute();
    }
}
