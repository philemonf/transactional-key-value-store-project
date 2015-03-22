package ch.epfl.tkvs.transactionmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class TMThread extends Thread {

	static String[] kv = new String[100]; // ONLY FOR TESTING

	private Socket socket = null;
	private Object valueRead = null;
	private int portNumber;

	public TMThread(Socket socket, int portNumber) {
		super("TMServerThread");
		this.socket = socket;
		this.portNumber = portNumber;
	}

	public void run() {
		try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);) {

			// Read the request into a JSONObject

			String inputStr;
			inputStr = in.readLine();

			JSONObject request = new JSONObject(inputStr);

			// Create the response
			JSONObject response = null;
			switch (request.getString("Type")) {
			case "Read":
				response = readRequest(request);
				break;
			case "Write":
				response = writeRequest(request);
				break;
			case "Commit":
				response = commitRequest(request);
				break;
			}

			// Send the response
			out.println(response.toString());

			in.close();
			out.close();
			socket.close();

		} catch (IOException | JSONException e) {
			e.printStackTrace();
		}
	}

	private JSONObject commitRequest(JSONObject request) throws JSONException {
		long transactionID = request.getLong("TransactionID");
		boolean success = commit(transactionID);

		JSONObject response = new JSONObject();
		response.put("Success", success);
		return response;
	}

	private boolean commit(long transactionID) {
		// TODO Auto-generated method stub
		return true;
	}

	private JSONObject writeRequest(JSONObject request) throws JSONException {
		long transactionID = request.getLong("TransactionID");
		String key = request.getString("Key");
		String value = request.getString("Value");
		boolean success = write(transactionID, key, value);

		JSONObject response = new JSONObject();
		response.put("Success", success);
		return response;
	}

	private boolean write(long transactionID, String key, String value) {
		// TODO Auto-generated method stub
		int k = new Integer(key);
		System.out.println("Write  " + key + "   " + value);
		kv[k] = value;
		return true;
	}

	private JSONObject readRequest(JSONObject request) throws JSONException {
		long transactionID = request.getLong("TransactionID");
		String key = request.getString("Key");
		boolean success = read(transactionID, key);

		JSONObject response = new JSONObject();
		response.put("Success", success);
		response.put("Value", valueRead);
		valueRead = null;
		return response;
	}

	private boolean read(long transactionID, String key) {
		// TODO Auto-generated method stub
		// Updates valueRead
		int k = new Integer(key);
		valueRead = kv[k];

		System.out.println("Read " + k + "   " + kv[k]);
		return true;
	}

}
