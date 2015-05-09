package ch.epfl.tkvs.transactionmanager.communication.requests;

import java.io.IOException;
import java.io.Serializable;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;


public class ReadRequest extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "read_request";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_TRANSACTION_ID)
    private int transactionId;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_KEY)
    private String encodedKey;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_HASH)
    private int localityHash;

    @JSONConstructor
    public ReadRequest(int transactionId, Serializable key, int hash) {
        this.transactionId = transactionId;
        try {
            this.encodedKey = Base64Utils.convertToBase64(key);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        this.localityHash = hash;
    }

    @Override
    public String toString() {
        String key;
        try {
            key = Base64Utils.convertFromBase64(encodedKey).toString();
        } catch (Exception ex) {
            key = encodedKey;
        }
        return MESSAGE_TYPE + " : t" + transactionId + "   key=" + key + "  hash=" + localityHash;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public String getEncodedKey() {
        return encodedKey;
    }

    public int getLocalityHash() {
        return localityHash;
    }
}
