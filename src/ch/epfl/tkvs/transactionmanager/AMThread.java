package ch.epfl.tkvs.transactionmanager;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.json.JSONException;
import org.json.JSONObject;

public class AMThread extends Thread {
	private Socket socket = null;
	private String valueRead = null;
	
	public AMThread(Socket socket) {
		super("TMServerThread");
                
		this.socket = socket;
	}
	
	public void run() {
		try (BufferedReader in = new BufferedReader(
				new InputStreamReader(socket.getInputStream(), "UTF-8"));
				PrintWriter out = new PrintWriter(
						socket.getOutputStream(), true);) {

			//Read the request into a JSONObject
			String inputStr;
                   
			inputStr = in.readLine();
                     

			JSONObject request = new JSONObject(inputStr);
			
			//Create the response
			JSONObject response = null;
			switch (request.getString("Type")) {
			case "TM":
				response = TMRequest(request);
				break;
			}
			
			//Send the response
			out.println(response.toString());
		
			in.close();
			out.close();
			socket.close();

		} catch (IOException | JSONException e) {
			e.printStackTrace();
		}
	}

	private JSONObject TMRequest(JSONObject request) throws JSONException {
		String key = request.getString("Key");
		
		// TODO Auto-generated method stub
		// Compute the hash of the key.
		
		String hostName = "localhost";
		int portNumber = TransactionManager.portNumber;
		long transactionID = 0;
		
		// get the hostName and portNumber for that hash.
		// create a unique transactionID
		System.out.println("Begin "+transactionID);
		JSONObject response = new JSONObject();
		response.put("HostName", hostName);
		response.put("PortNumber", portNumber);
		response.put("TransactionID", transactionID);
		return response;
	}
}

