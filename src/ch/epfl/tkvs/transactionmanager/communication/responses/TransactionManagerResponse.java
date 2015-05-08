package ch.epfl.tkvs.transactionmanager.communication.responses;

import java.io.IOException;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;


public class TransactionManagerResponse extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "transaction_manager_response";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_SUCCESS)
    private boolean success;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_TRANSACTION_ID)
    private int transactionId;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_IP)
    private String ip;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_PORT)
    private Integer port;

    @JSONConstructor
    public TransactionManagerResponse(boolean success, int transactionId, String ip, Integer port) throws IOException {
        this.success = success;
        this.transactionId = transactionId;
        this.port = port;
        this.ip = ip;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public int getTransactionId() {
        return transactionId;
    }

    @Override
    public String toString() {
        return MESSAGE_TYPE + " : " + success + "  id=" + transactionId + "  ip=" + ip + "  port=" + port;
    }
}
