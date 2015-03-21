package ch.epfl.tkvs.userclient;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import org.json.JSONException;
import org.json.JSONObject;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author sachin
 */
public class Transaction
  {

    private static String hostNameOfAM;
    private static int portNumberOfAM;
    private String hostNameOfTM = null;
    private int portNumberOfTM;
    private long transactionID;
    private TransactionStatus status;

    public static void initialize(String host, int port)
      {
        hostNameOfAM = host;
        portNumberOfAM = port;

      }

    public Transaction(MyKey key)
      {
        try
          {
            JSONObject request = new JSONObject();
            request.put("Type", "TM");
            request.put("Key", key);
            JSONObject response = sendRequest(hostNameOfAM, portNumberOfAM, request);
            hostNameOfTM = response.getString("HostName");
            portNumberOfTM = response.getInt("PortNumber");
            transactionID = response.getLong("TransactionID");
            status = TransactionStatus.live;
          } catch (JSONException e)
          {
            hostNameOfTM = null;
            e.printStackTrace();
          }
      }

    private JSONObject sendRequest(String hostName, int portNumber, JSONObject request)
      {
        try (Socket socket = new Socket(hostName, portNumber);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(),true);)
          {
            out.println(request.toString());
            
            System.out .print("Sending request to " + hostName + ":" + portNumber+"     ");
            System.out.println(request.toString());
            
            //Read the response into a JSONObject
            
            String inputStr=in.readLine();
            System.out.print("Received response from " + hostName + ":" + portNumber+"     ");
            System.out.println(inputStr);
            

            JSONObject response = new JSONObject(inputStr);

            return response;

          } catch (UnknownHostException e)
          {
            System.err.println("Don't know about host " + hostName);
            System.exit(1);
          } catch (IOException e)
          {
            System.err.println("Couldn't get I/O for the connection to " + hostName+":"+portNumber);
            e.printStackTrace();
            System.exit(1);
          } catch (JSONException e)
          {
            e.printStackTrace();
          }
        return null;
      }

    public MyValue read(MyKey key) throws AbortException, JSONException
      {

        if (status != TransactionStatus.live)
          {
            throw new AbortException("Transaction is no longer live");
          }
        JSONObject request = new JSONObject();

        request.put("Type", "Read");
        request.put("TransactionID", transactionID);
        request.put("Key", key.toString());
        JSONObject response = sendRequest(hostNameOfTM, portNumberOfTM, request);

        boolean isSuccess = response.getBoolean("Success");
        if (!isSuccess)
          {
            status = TransactionStatus.aborted;
            throw new AbortException("Abort");
          };
        return new MyValue(response.getString("Value"));
      }

    public void write(MyKey key, MyValue value) throws AbortException, JSONException
      {

        if (status != TransactionStatus.live)
          {
            throw new AbortException("Transaction is no longer live");
          }
        JSONObject request = new JSONObject();
        request.put("Type", "Write");
        request.put("TransactionID", transactionID);
        request.put("Key", key.toString());
        request.put("Value", value.toString());
        JSONObject response = sendRequest(hostNameOfTM, portNumberOfTM, request);
        boolean isSuccess = response.getBoolean("Success");
        if (!isSuccess)
          {
            status = TransactionStatus.aborted;
            throw new AbortException("Abort");
          }
      }

    private void commit() throws AbortException, JSONException

      {

        if (status != TransactionStatus.live)
          {
            throw new AbortException("Transaction is no longer live");
          }
        JSONObject request = new JSONObject();
        request.put("Type", "Commit");
        request.put("TransactionID", transactionID);
        JSONObject response = sendRequest(hostNameOfTM, portNumberOfTM, request);
        boolean isSuccess = response.getBoolean("Success");
        if (!isSuccess)
          {
            status = TransactionStatus.aborted;
            throw new AbortException("Abort");

          }
      }

  }
