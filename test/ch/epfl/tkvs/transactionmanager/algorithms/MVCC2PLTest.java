package ch.epfl.tkvs.transactionmanager.algorithms;

import ch.epfl.tkvs.transactionmanager.communication.requests.BeginRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.CommitRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.ReadRequest;
import ch.epfl.tkvs.transactionmanager.communication.requests.WriteRequest;
import ch.epfl.tkvs.transactionmanager.communication.responses.GenericSuccessResponse;
import ch.epfl.tkvs.transactionmanager.communication.responses.ReadResponse;
import java.io.IOException;

import junit.framework.TestCase;
import org.junit.Test;

public class MVCC2PLTest extends TestCase {
	private static MVCC2PL concurrentInstance = new MVCC2PL();

	@Test
	public void testRead() {
		try {

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

		} catch (IOException ex) {
			fail(ex.getLocalizedMessage());
		}
	}

	/**
	 * Test of write method, of class MVCC2PL.
	 */
	@Test
	public void testWrite() {

		try {
			WriteRequest request = new WriteRequest(0, 0, 0, 0);
			MVCC2PL instance = new MVCC2PL();

			GenericSuccessResponse result = instance.write(request);
			assertEquals(false, result.getSuccess());

			GenericSuccessResponse br;
			br = instance.begin(new BeginRequest(0));
			assertEquals(true, br.getSuccess());

			result = instance.write(request);
			assertEquals(true, result.getSuccess());

		} catch (IOException ex) {
			fail(ex.getLocalizedMessage());
		}
	}

	/**
	 * Test of begin method, of class MVCC2PL.
	 */
	@Test
	public void testBegin() {

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
	public void testCommit() {

		try {
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

			// WHAT SHOULD BE RESULT, true or false? At the moment it will
			// succeed and test will fail
			result = instance.begin(new BeginRequest(0));
			assertEquals(true, result.getSuccess());

		} catch (IOException ex) {
			fail(ex.getLocalizedMessage());
		}

	}

	@Test
	public void testSingle() {
		try {
			MVCC2PL instance = new MVCC2PL();

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

		} catch (IOException ex) {
			fail(ex.getLocalizedMessage());
		}

	}

	@Test
	public void testSerial() {
		try {
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
		} catch (IOException ex) {
			fail(ex.getLocalizedMessage());
		}

	}

	@Test
	public void testDeadlock() {

		Thread thread1 = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					GenericSuccessResponse gsr;
					ReadResponse rr;

					gsr = concurrentInstance.begin(new BeginRequest(1));
					assertEquals(true, gsr.getSuccess());

					rr = concurrentInstance.read(new ReadRequest(1, 1, 0));
					assertEquals(true, rr.getSuccess());
					assertEquals(null, rr.getValue());

					Thread.sleep(2000);

					gsr = concurrentInstance.write(new WriteRequest(1, 2, "y1",
							0));
					assertEquals(true, gsr.getSuccess());

					Thread.sleep(2000);

					gsr = concurrentInstance.commit(new CommitRequest(1));
					assertEquals(true, gsr.getSuccess());

				} catch (Exception ex) {
					fail(ex.getLocalizedMessage());
				}
			}
		});

		Thread thread2 = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					GenericSuccessResponse gsr;
					ReadResponse rr;

					Thread.sleep(1000);

					gsr = concurrentInstance.begin(new BeginRequest(2));
					assertEquals(true, gsr.getSuccess());

					rr = concurrentInstance.read(new ReadRequest(2, 2, 0));
					assertEquals(true, rr.getSuccess());
					assertEquals(null, rr.getValue());

					Thread.sleep(2000);

					gsr = concurrentInstance.write(new WriteRequest(2, 1, "x2",
							0));
					assertEquals(true, gsr.getSuccess());

					Thread.sleep(10000); // I make it wait long to make sure
											// that I give time to the thread1

					gsr = concurrentInstance.commit(new CommitRequest(2));
					assertEquals(false, gsr.getSuccess());

				} catch (Exception ex) {
					fail(ex.getLocalizedMessage());
				}
			}
		});

		thread1.start();
		thread2.start();

		try {
			// thread1.join();
			thread2.join();
		} catch (InterruptedException ex) {
			fail(ex.getLocalizedMessage());
		}
	}
}
