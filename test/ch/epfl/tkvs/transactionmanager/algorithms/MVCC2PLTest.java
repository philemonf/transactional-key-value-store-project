package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import java.io.IOException;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import org.junit.Test;

public class MVCC2PLTest
  {

    @Test
    public void testRead()
      {
        try
          {

            ReadRequest request = new ReadRequest(0, 0, 0);
            MVCC2PL instance = new MVCC2PL();

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
            MVCC2PL instance = new MVCC2PL();

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
        MVCC2PL instance = new MVCC2PL();

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

            MVCC2PL instance = new MVCC2PL();

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

            //WHAT SHOULD BE RESULT, true or false?  At the moment it will succeed and test will fail
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
            MVCC2PL instance = new MVCC2PL();

            GenericSuccessResponse br = instance.begin(new BeginRequest(1));
            assertEquals(true, br.getSuccess());

            GenericSuccessResponse wr = instance.write(new WriteRequest(1, 0, "zero", 0));
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
            MVCC2PL instance = new MVCC2PL();

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
        try
          {
            MVCC2PL instance = new MVCC2PL();

            GenericSuccessResponse gsr;
            ReadResponse rr;

            gsr = instance.begin(new BeginRequest(1));
            assertEquals(true, gsr.getSuccess());

            gsr = instance.begin(new BeginRequest(2));
            assertEquals(true, gsr.getSuccess());

            rr = instance.read(new ReadRequest(1, 1, 0));
            assertEquals(true, rr.getSuccess());
            assertEquals(null, rr.getValue());

            rr = instance.read(new ReadRequest(2, 2, 0));
            assertEquals(true, rr.getSuccess());
            assertEquals(null, rr.getValue());

            gsr = instance.write(new WriteRequest(1, 2, "y1", 0));
            assertEquals(true, gsr.getSuccess());

            gsr = instance.write(new WriteRequest(2, 1, "x2", 0));
            assertEquals(true, gsr.getSuccess());

//            gsr = instance.commit(new CommitRequest(1));
//            assertEquals(true, gsr.getSuccess());

//            gsr = instance.commit(new CommitRequest(2));
//            assertEquals(true, gsr.getSuccess());

          } catch (IOException ex)
          {
            fail(ex.getLocalizedMessage());
          }
      }
  }
