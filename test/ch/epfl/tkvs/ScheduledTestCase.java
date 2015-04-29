package ch.epfl.tkvs;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import junit.framework.TestCase;


/**
 * Created with IntelliJ IDEA. User: philemonfavrod Date: 27/04/15 Time: 20:24 To change this template use File |
 * Settings | File Templates.
 */
public abstract class ScheduledTestCase extends TestCase {

    public static abstract class ScheduledCommand {

        private ScheduledCommand[][] schedule;

        public void setSchedule(ScheduledCommand[][] schedule) {
            this.schedule = schedule;
        }

        public ScheduledCommand[][] getSchedule() {
            return this.schedule;
        }

        public abstract void perform();

        protected void internalPerform(final CyclicBarrier barrier) throws InterruptedException, BrokenBarrierException {
            perform();
            barrier.await();
        }

        protected int getOrder() {
            return 0;
        }
    }

    public static abstract class ScheduledCommandWithAssertion extends ScheduledCommand {

        public abstract void assertBefore();

        public abstract void assertAfter();

        @Override
        protected void internalPerform(final CyclicBarrier barrier) throws InterruptedException, BrokenBarrierException {
            assertBefore();
            perform();
            barrier.await();
            assertAfter();
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

            // Wait a bit hoping that the thread will be blocked or to terminate
            Thread.sleep(5000);

            barrier.await();
        }

        public Thread getPerformThread() {
            return performThread;
        }
    }

    public static final ScheduledCommand ___ = new ScheduledCommand() {

        @Override
        public void perform() {

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

            if (!blockingCommand.getPerformThread().isAlive()) {
                System.out.println("SHIIT");
            }

            assertEquals(true, blockingCommand.getPerformThread().isAlive());
        }
    }

    public static ScheduledCommand Wt(int tid, int step) {
        return new ShouldWaitScheduledCommand(tid, step);
    }

    public static class ScheduleExecutor {

        private ScheduledCommand[][] schedule;

        public ScheduleExecutor(ScheduledCommand[][] schedule) {
            if (schedule.length < 1) {
                throw new IllegalArgumentException("The schedule shouldn't be empty.");
            }

            this.schedule = schedule;
        }

        public void execute() {

            int numThreads = this.schedule.length;
            Thread[] executorThreads = new Thread[numThreads];

            final CyclicBarrier barrier = new CyclicBarrier(numThreads);

            final int numSteps = this.schedule[0].length;

            for (int i = 0; i < numThreads; ++i) {
                final int tid = i;
                executorThreads[tid] = new Thread(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            for (int step = 0; step < numSteps; ++step) {
                                schedule[tid][step].setSchedule(schedule);
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

            for (int i = 0; i < numThreads; i++) {
                executorThreads[i].start();
            }

            for (int i = 0; i < numThreads; i++) {
                try {
                    executorThreads[i].join();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
}
