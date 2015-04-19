package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import java.io.IOException;

import junit.framework.TestCase;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import org.junit.Before;
import org.junit.Test;

public class MVCC2PLTest extends TestCase
  {

    private static MVCC2PL instance = new MVCC2PL();

    @Before
    public void setUp() throws Exception
      {
        System.out.println("Setting up");
        instance = new MVCC2PL();
      }



    @Test
    public void testRead()
      {
        try
          {

            ReadRequest request = new ReadRequest(0, 0, 0);

            ReadResponse result = instance.read(request);
            assertEquals(false, result.getSuccess());

            GenericSuccessResponse br;
            br = instance.begin(new BeginRequest(0));
            assertEquals(true, br.getSuccess());

            result = instance.read(request);
            assertEquals(true, result.getSuccess());
            assertEquals(null, result.getValue());

          } catch (IOException ex)
          {
            fail(ex.getLocalizedMessage());
          }
      }

    /**
     * Test of write method, of class MVCC2PL.
     */
    @Test
    public void testWrite()
      {

        try
          {
            WriteRequest request = new WriteRequest(0, 0, 0, 0);

            GenericSuccessResponse result = instance.write(request);
            assertEquals(false, result.getSuccess());

            GenericSuccessResponse br;
            br = instance.begin(new BeginRequest(0));
            assertEquals(true, br.getSuccess());

            result = instance.write(request);
            assertEquals(true, result.getSuccess());

          } catch (IOException ex)
          {
            fail(ex.getLocalizedMessage());
          }
      }

    /**
     * Test of begin method, of class MVCC2PL.
     */
    @Test
    public void testBegin()
      {

        BeginRequest request = new BeginRequest(0);

        GenericSuccessResponse result = instance.begin(request);
        assertEquals(true, result.getSuccess());

        result = instance.begin(request);
        assertEquals(false, result.getSuccess());
      }

    /**
     * Test of commit method, of class MVCC2PL.
     */
    @Test
    public void testCommit()
      {

        try
          {
            CommitRequest request = new CommitRequest(0);

            GenericSuccessResponse result = instance.commit(request);
            assertEquals(false, result.getSuccess());

            GenericSuccessResponse br;
            br = instance.begin(new BeginRequest(0));
            assertEquals(true, br.getSuccess());

            result = instance.commit(request);
            assertEquals(true, result.getSuccess());

            result = instance.commit(request);
            assertEquals(false, result.getSuccess());

            ReadResponse rr = instance.read(new ReadRequest(0, 0, 0));
            assertEquals(false, rr.getSuccess());

            result = instance.write(new WriteRequest(0, 0, 0, 0));
            assertEquals(false, result.getSuccess());

            // WHAT SHOULD BE RESULT, true or false? At the moment it will
            // succeed and test will fail
            result = instance.begin(new BeginRequest(0));
            assertEquals(true, result.getSuccess());

          } catch (IOException ex)
          {
            fail(ex.getLocalizedMessage());
          }

      }

    @Test
    public void testSingle()
      {
        try
          {

            GenericSuccessResponse br = instance.begin(new BeginRequest(1));
            assertEquals(true, br.getSuccess());

            GenericSuccessResponse wr = instance.write(new WriteRequest(1, 0,
                    "zero", 0));
            assertEquals(true, wr.getSuccess());

            ReadResponse rr = instance.read(new ReadRequest(1, 0, 0));
            assertEquals(true, rr.getSuccess());
            assertEquals("zero", (String) rr.getValue());

            GenericSuccessResponse cr = instance.commit(new CommitRequest(1));
            assertEquals(true, cr.getSuccess());

          } catch (IOException ex)
          {
            fail(ex.getLocalizedMessage());
          }

      }

    @Test
    public void testSerial()
      {
        try
          {

            GenericSuccessResponse gsr = instance.begin(new BeginRequest(0));
            assertEquals(true, gsr.getSuccess());

            gsr = instance.write(new WriteRequest(0, 0, "zero", 0));
            assertEquals(true, gsr.getSuccess());

            gsr = instance.write(new WriteRequest(0, 1, "one", 0));
            assertEquals(true, gsr.getSuccess());

            gsr = instance.commit(new CommitRequest(0));
            assertEquals(true, gsr.getSuccess());

            gsr = instance.begin(new BeginRequest(1));
            assertEquals(true, gsr.getSuccess());

            ReadResponse rr = instance.read(new ReadRequest(1, 1, 0));
            assertEquals(true, rr.getSuccess());
            assertEquals("one", (String) rr.getValue());

            rr = instance.read(new ReadRequest(1, 0, 0));
            assertEquals(true, rr.getSuccess());
            assertEquals("zero", (String) rr.getValue());

            gsr = instance.commit(new CommitRequest(1));
            assertEquals(true, gsr.getSuccess());
          } catch (IOException ex)
          {
            fail(ex.getLocalizedMessage());
          }

      }

    @Test
    public void testDeadlock()
      {

        Thread thread1 = new Thread(new Runnable()
          {

            @Override
            public void run()
              {
                try
                  {
                    GenericSuccessResponse gsr;
                    ReadResponse rr;

                    gsr = instance.begin(new BeginRequest(1));
                    assertEquals(true, gsr.getSuccess());

                    rr = instance.read(new ReadRequest(1, 1, 0));
                    assertEquals(true, rr.getSuccess());
                    assertEquals(null, rr.getValue());

                    Thread.sleep(2000);

                    gsr = instance.write(new WriteRequest(1, 2, "y1",
                            0));
                    assertEquals(true, gsr.getSuccess());

                    Thread.sleep(2000);

                    gsr = instance.commit(new CommitRequest(1));
                    assertEquals(true, gsr.getSuccess());

                  } catch (Exception ex)
                  {
                    fail(ex.getLocalizedMessage());
                  }
              }
          });

        Thread thread2 = new Thread(new Runnable()
          {

            @Override
            public void run()
              {
                try
                  {
                    GenericSuccessResponse gsr;
                    ReadResponse rr;

                    Thread.sleep(1000);

                    gsr = instance.begin(new BeginRequest(2));
                    assertEquals(true, gsr.getSuccess());

                    rr = instance.read(new ReadRequest(2, 2, 0));
                    assertEquals(true, rr.getSuccess());
                    assertEquals(null, rr.getValue());

                    Thread.sleep(2000);

                    gsr = instance.write(new WriteRequest(2, 1, "x2",
                            0));
                    assertEquals(true, gsr.getSuccess());

                    Thread.sleep(10000); // I make it wait long to make sure
                    // that I give time to the thread1

                    gsr = instance.commit(new CommitRequest(2));
                    assertEquals(false, gsr.getSuccess());

                  } catch (Exception ex)
                  {
                    fail(ex.getLocalizedMessage());
                  }
              }
          });

        thread1.start();
        thread2.start();

        try
          {
            thread1.join();
            thread2.join();
          } catch (InterruptedException ex)
          {
            fail(ex.getLocalizedMessage());
          }
      }

    @Test
    public void testComplex()
      {
        Thread t1 = new Thread(new Runnable()
          {

            @Override
            public void run()
              {
                try
                  {
                    GenericSuccessResponse gsr;
                    ReadResponse rr;

                    gsr = instance.begin(new BeginRequest(1));
                    assertEquals(gsr.getSuccess(), true);

                    rr = instance.read(new ReadRequest(1, "x", 0));
                    assertEquals(true, rr.getSuccess());
                    assertEquals(null, rr.getValue());

                    Thread.sleep(1000);

                    rr = instance.read(new ReadRequest(1, "y", 0));
                    assertEquals(true, rr.getSuccess());
                    assertEquals(null, rr.getValue());

                    gsr = instance.write(new WriteRequest(1, "x", "x1", 0));
                    assertEquals(gsr.getSuccess(), true);

                    gsr = instance.commit(new CommitRequest(1));
                    assertEquals(gsr.getSuccess(), true);

                  } catch (IOException | InterruptedException ex)
                  {
                    fail(ex.getLocalizedMessage());
                  }

              }
          });

        Thread t2 = new Thread(new Runnable()
          {

            @Override
            public void run()
              {
                try
                  {
                    GenericSuccessResponse gsr;

                    gsr = instance.begin(new BeginRequest(2));
                    assertEquals(gsr.getSuccess(), true);

                    gsr = instance.write(new WriteRequest(2, "y", "y2", 0));
                    assertEquals(gsr.getSuccess(), true);
                    Thread.sleep(4500);
                    gsr = instance.write(new WriteRequest(2, "x", "x2", 0));
                    assertEquals(gsr.getSuccess(), true);

                    gsr = instance.commit(new CommitRequest(2));
                    assertEquals(gsr.getSuccess(), true);

                  } catch (IOException | InterruptedException ex)
                  {
                    fail(ex.getLocalizedMessage());
                  }

              }
          });

        Thread t3 = new Thread(new Runnable()
          {

            @Override
            public void run()
              {
                try
                  {
                    GenericSuccessResponse gsr;
                    ReadResponse rr;

                    gsr = instance.begin(new BeginRequest(3));
                    assertEquals(gsr.getSuccess(), true);

                    rr = instance.read(new ReadRequest(3, "y", 0));
                    assertEquals(true, rr.getSuccess());
                    assertEquals(null, rr.getValue());

                    rr = instance.read(new ReadRequest(3, "z", 0));
                    assertEquals(true, rr.getSuccess());
                    assertEquals(null, rr.getValue());

                    gsr = instance.write(new WriteRequest(3, "z", "z3", 0));
                    assertEquals(gsr.getSuccess(), true);
                    Thread.sleep(2200);
                    gsr = instance.commit(new CommitRequest(3));
                    assertEquals(gsr.getSuccess(), true);

                  } catch (IOException | InterruptedException ex)
                  {
                    fail(ex.getLocalizedMessage());
                  }

              }
          });

        Thread t4 = new Thread(new Runnable()
          {

            @Override
            public void run()
              {
                try
                  {
                    GenericSuccessResponse gsr;

                    gsr = instance.begin(new BeginRequest(4));
                    assertEquals(gsr.getSuccess(), true);

                    gsr = instance.write(new WriteRequest(4, "z", "z4", 0));
                    assertEquals(gsr.getSuccess(), true);

                    gsr = instance.commit(new CommitRequest(4));
                    assertEquals(gsr.getSuccess(), true);

                  } catch (IOException ex)
                  {
                    fail(ex.getLocalizedMessage());
                  }

              }
          });
        try
          {

//            t1.start();
            Thread.sleep(500);
//            t2.start();
            Thread.sleep(3000);
            t3.start();
            Thread.sleep(2000);
            t4.start();

//            t1.join();
//            t2.join();
            t3.join();
            t4.join();

          } catch (Exception ex)
          {
            fail(ex.getLocalizedMessage());
          }
      }
  }
