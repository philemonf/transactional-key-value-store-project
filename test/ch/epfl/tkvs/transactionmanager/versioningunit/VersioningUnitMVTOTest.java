package ch.epfl.tkvs.transactionmanager.versioningunit;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class VersioningUnitMVTOTest {

    @Before
    public void setUp() throws Exception {
        VersioningUnitMVTO.instance.init();
    }

    @After
    public void tearDown() throws Exception {
    }

    private static PrintStream log = System.out;

    @Test
    public void test1() {
        log.println("----------- Test 1 -----------");
        int[][][] schedule = new int[][][] {
        /* T1: */{ W(1), __C_ },
        /* T2: */{ ____, ____, R(1), ____, R(1), ____, __C_ },
        /* T3: */{ ____, ____, ____, W(1), ____, R(1), ____, __C_ },
        /* T4: */{ ____, ____, ____, ____, ____, ____, ____, ____, R(1), __C_}};
        
        // T(1):W(1,4)
        // T(1):COMMIT SUCCESSFUL
        // T(2):R(1) => 4
        // T(3):W(1,10)
        // T(2):R(1) => 4
        // T(3):R(1) => 10
        // T(2):COMMIT SUCCESSFUL
        // T(3):COMMIT SUCCESSFUL
        // T(4):R(1) => 10
        // T(4):COMMIT SUCCESSFUL
        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][] expectedResults = new Object[schedule.length][maxLen];
        expectedResults[T(2)][STEP(3)] = STEP(1);
        expectedResults[T(2)][STEP(5)] = STEP(1);
        expectedResults[T(3)][STEP(6)] = STEP(4);
        expectedResults[T(4)][STEP(9)] = STEP(4);
        executeSchedule(schedule, expectedResults, maxLen);
    }

    /**
     * This method is for executing a schedule.
     * 
     * @param schedule is a 3D array containing one transaction in each row, and in each cell is one operation
     * @param expectedResults is the array of expected result in each READ operation. For: - READ: the cell contains the
     * STEP# (zero-based) in the schedule that WRITTEN the value that should be read here.
     * @param maxLen is the maximum length of schedule
     */
    private static void executeSchedule(int[][][] schedule, Object[][] expectedResults, int maxLen) {
        Map<Integer, Integer> xactLabelToXact = new HashMap<Integer, Integer>();
        Set<Integer> ignoredXactLabels = new HashSet<Integer>();

        for (int step = 0; step < maxLen; step++) {
            for (int i = 0; i < schedule.length; i++) {
                if (step < schedule[i].length && schedule[i][step] != null) {
                    int[] xactOps = schedule[i][step];
                    int xactLabel = i + 1;
                    if (ignoredXactLabels.contains(xactLabel))
                        break;

                    int xact = 0;
                    try {
                        if (xactLabelToXact.containsKey(xactLabel)) {
                            xact = xactLabelToXact.get(xactLabel);
                        } else {
                            xact = VersioningUnitMVTO.instance.begin_transaction(-1);
                            xactLabelToXact.put(xactLabel, xact);
                        }
                        if (xactOps.length == 1) {
                            switch (xactOps[0]) {
                            case COMMIT:
                                VersioningUnitMVTO.instance.commit(xact);
                                break;
                            case ABORT:
                                VersioningUnitMVTO.instance.abort(xact);
                                break;
                            }
                        } else {
                            switch (xactOps[0]) {
                            case WRITE:
                                VersioningUnitMVTO.instance.put(xact, xactOps[1], getValue(step));
                                break;
                            case READ: {
                                int readValue = 0;
                                Serializable readValueTmp = VersioningUnitMVTO.instance.get(xact, xactOps[1]);
                                if (readValueTmp != null) {
                                    readValue = (int) readValueTmp;
                                }
                                int expected = getValue((Integer) expectedResults[T(xactLabel)][step]);
                                if (readValue != expected) {
                                    throw new WrongResultException(xactLabel, step, xactOps, readValue, expected);
                                }
                                break;
                            }
                            }
                        }
                    } catch (WrongResultException e) {
                        throw e;
                    } catch (Exception e) {
                        ignoredXactLabels.add(xactLabel);
                        if (xactOps[0] == READ)
                            log.println();
                        if (e.getMessage() != null)
                            log.println("    " + e.getMessage());
                        else
                            e.printStackTrace();
                    }
                    break;
                }
            }
        }
    }

    /**
     * @param step is the STEP# in the schedule (zero-based)
     * @return the expected result of a READ operation in a schedule.
     */
    private static int getValue(int step) {
        return (step + 2) * 2;
    }

    private static void printSchedule(int[][][] schedule) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schedule.length; i++) {
            sb.append("T").append(i + 1).append(": ");
            for (int j = 0; j < schedule[i].length; j++) {
                int[] xactOps = schedule[i][j];
                if (xactOps == null) {
                    sb.append("     ");
                } else if (xactOps.length == 1) {
                    switch (xactOps[0]) {
                    case COMMIT:
                        sb.append("  C ");
                        break;
                    case ABORT:
                        sb.append("  A ");
                        break;
                    }
                } else {
                    switch (xactOps[0]) {
                    case WRITE:
                        sb.append("W");
                        break;
                    case READ:
                        sb.append("R");
                        break;
                    }
                    sb.append("(").append(xactOps[1]).append(")");
                }
                if (j + 1 < schedule[i].length && xactOps != null) {
                    sb.append(",");
                }
            }
            sb.append("\n");
        }
        log.println("\n" + sb.toString());
    }

    /**
     * Analyzes and validates the given schedule.
     * 
     * @return maximum number of steps in the transactions inside the given schedule
     */
    private static int analyzeSchedule(int[][][] schedule) {
        int maxLen = 0;
        for (int i = 0; i < schedule.length; i++) {
            if (maxLen < schedule[i].length) {
                maxLen = schedule[i].length;
            }
            for (int j = 0; j < schedule[i].length; j++) {
                int[] xactOps = schedule[i][j];
                if (xactOps == null) {
                    // no operation
                } else if (xactOps.length == 1 && (xactOps[0] == COMMIT || xactOps[0] == ABORT)) {
                    // commit or roll back
                } else if (xactOps.length == 2) {
                    switch (xactOps[0]) {
                    case WRITE: /* write */
                        ;
                        break;
                    case READ: /* read */
                        ;
                        break;
                    default:
                        throw new RuntimeException("Unknown operation in schedule: T" + (i + 1) + ", Operation " + (j + 1));
                    }
                } else {
                    throw new RuntimeException("Unknown operation in schedule: T" + (i + 1) + ", Operation " + (j + 1));
                }
            }
        }
        return maxLen;
    }

    private final static int /* BEGIN = 1, */WRITE = 2, READ = 3, COMMIT = 4, ABORT = 5;
    private final static int[] /* __B_ = {BEGIN}, */__C_ = { COMMIT }, __A_ = { ABORT }, ____ = null;

    // transaction
    private static int T(int i) {
        return i - 1;
    }

    // step
    private static int STEP(int i) {
        return i - 1;
    }

    // write
    public static int[] W(int key) {
        return new int[] { WRITE, key };
    }

    // read
    public static int[] R(int key) {
        return new int[] { READ, key };
    }

    static class WrongResultException extends RuntimeException {

        private static final long serialVersionUID = -7630223385777784923L;

        public WrongResultException(int xactLabel, int step, int[] operation, Object actual, Object expected) {
            super("Wrong result in T(" + xactLabel + ") in step " + (step + 1) + " (Actual: " + actual + ", Expected: " + expected + ")");
        }
    }

}
