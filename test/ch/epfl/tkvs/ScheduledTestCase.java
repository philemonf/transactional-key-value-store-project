package ch.epfl.tkvs;

import junit.framework.TestCase;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * Created with IntelliJ IDEA.
 * User: philemonfavrod
 * Date: 27/04/15
 * Time: 20:24
 * To change this template use File | Settings | File Templates.
 */
public class ScheduledTestCase extends TestCase {
    public static abstract class ScheduledCommand {
        private ScheduledCommand[][] schedule;

        public void setSchedule(ScheduledCommand[][] schedule) {
            this.schedule = schedule;
        }

        public ScheduledCommand[][] getSchedule() {
            return this.schedule;
        }

        abstract void perform();

        protected void internalPerform(final CyclicBarrier barrier) throws InterruptedException, BrokenBarrierException {
            perform();
            barrier.await();
        }
    }

    public static abstract class ScheduledCommandWithAssertion extends ScheduledCommand {
        abstract void assertBefore();
        abstract void assertAfter();

        @Override
        protected void internalPerform(final CyclicBarrier barrier) throws InterruptedException, BrokenBarrierException {
            perform();
            barrier.await();
        }
    }

    public static abstract class ScheduledBlockingCommand extends ScheduledCommand {
        private Thread performThread = null;
        @Override
        protected void internalPerform(final CyclicBarrier barrier) throws InterruptedException, BrokenBarrierException {
            performThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    perform();
                    try {
                        barrier.await();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            performThread.start();
            barrier.await();
        }

        public Thread getPerformThread() {
            return performThread;
        }
    }



    public static final ScheduledCommand ___ = new ScheduledCommand() {
        @Override
        void perform() {

        }
    };

    public static class ShouldWaitScheduledCommand extends ScheduledCommand {
        private int tid;
        private int step;

        public ShouldWaitScheduledCommand(int tid, int step) {
            this.tid = tid;
            this.step = step;
        }

        public void perform() {
            assert getSchedule() != null : "Problem in the test framework: schedule should never be null!";

            ScheduledBlockingCommand blockingCommand = (ScheduledBlockingCommand) getSchedule()[tid][step];

            assertEquals(blockingCommand.getPerformThread().isAlive(), true);
        }
    }

    public static ScheduledCommand W(int tid, int step) {
        return new ShouldWaitScheduledCommand(tid, step);
    }



    public static class ScheduleExecutor {

        private ScheduledCommand[][] schedule;

        public ScheduleExecutor(ScheduledCommand[][] schedule) {
            if (this.schedule.length < 1) {
                throw new IllegalArgumentException("The schedule shouldn't be empty.");
            }

            this.schedule = schedule;
        }

        public void execute() {

            int numThreads = this.schedule.length;
            Thread [] executorThreads = new Thread[numThreads];

            final CyclicBarrier barrier = new CyclicBarrier(numThreads);

            final int numSteps = this.schedule[0].length;

            for (int i = 0; i < numThreads; ++i) {
                final int tid = i;
                executorThreads[tid] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (int step = 0; step < numSteps; ++step) {
                                schedule[tid][step].internalPerform(barrier);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (BrokenBarrierException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
    }
}
