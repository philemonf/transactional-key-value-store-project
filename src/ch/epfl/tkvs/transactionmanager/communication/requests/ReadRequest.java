package ch.epfl.tkvs.transactionmanager.communication.requests;

import java.io.IOException;
import java.io.Serializable;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
import org.apache.log4j.Logger;


public class ReadRequest extends Message {

    private static Logger log = Logger.getLogger(ReadRequest.class);

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "read_request";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_TRANSACTION_ID)
    private int transactionId;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_KEY)
    private String encodedKey;

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_HASH)
    private int TMhash;

    public ReadRequest(int transactionId, Serializable key, int hash) {
        this.transactionId = transactionId;
        try {
            this.encodedKey = Base64Utils.convertToBase64(key);
        } catch (IOException ex) {
            log.fatal("Cannot encode key", ex);
            this.encodedKey = null;
        }
        this.TMhash = hash;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public String getEncodedKey() {
        return encodedKey;
    }

    public int getTMhash() {
        return TMhash;
    }
}
