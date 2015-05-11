package ch.epfl.tkvs.transactionmanager.communication.requests;

import java.io.IOException;
import java.io.Serializable;

import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;


/**
 * This message is sent to a {@link TransactionManager} to write value to a key.
 * 
 */
public class WriteRequest extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "write_request";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_TRANSACTION_ID)
    private int transactionId;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_KEY)
    private String encodedKey;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_VALUE)
    private String encodedValue;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_HASH)
    private int localityHash;

    @JSONConstructor
    public WriteRequest(int transactionId, Serializable key, Serializable value, int hash) {

        this.transactionId = transactionId;
        this.localityHash = hash;
        try {
            this.encodedKey = Base64Utils.convertToBase64(key);
            this.encodedValue = Base64Utils.convertToBase64(value);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public String toString()

    {
        String key;
        String value;

        try {
            key = Base64Utils.convertFromBase64(encodedKey).toString();
        } catch (Exception ex) {
            key = encodedKey;
        }
        try {
            value = Base64Utils.convertFromBase64(encodedValue).toString();
        } catch (Exception ex) {
            value = encodedValue;
        }
        return MESSAGE_TYPE + " : t" + transactionId + "  " + key + "  value=" + value;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public String getEncodedKey() {
        return encodedKey;
    }

    public String getEncodedValue() {
        return encodedValue;
    }

    public int getLocalityHash() {
        return localityHash;
    }
}
