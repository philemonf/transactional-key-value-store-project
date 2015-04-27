package ch.epfl.tkvs;

import junit.framework.Test;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: philemonfavrod
 * Date: 27/04/15
 * Time: 21:06
 * To change this template use File | Settings | File Templates.
 */
public class ScheduledExampleTestCase extends ScheduledTestCase {

    final Lock lock = new ReentrantLock();

    public ScheduledBlockingCommand L() {
        return new ScheduledBlockingCommand() {
            @Override
            void perform() {
                lock.lock();
            }
        };
    }

    public ScheduledCommand U() {
        return new ScheduledCommand() {
            @Override
            void perform() {
                lock.unlock();
            }
        };
    }


    @Test
    public void test() {
        ScheduledCommand[][] schedule = {
            {   L(),   W(0,0),  U(),     ___ },
            {   ___,   L(),     W(1,1),  U() }
        };
        ScheduleExecutor executor = new ScheduleExecutor(schedule);
        executor.execute();
    }
}
