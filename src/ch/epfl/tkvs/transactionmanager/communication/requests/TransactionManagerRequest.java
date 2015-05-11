package ch.epfl.tkvs.transactionmanager.communication.requests;

import ch.epfl.tkvs.test.userclient.UserClient;
import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONCommunication;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;
import ch.epfl.tkvs.yarn.appmaster.AppMaster;


/**
 * This message is sent to {@link AppMaster} from a {@link UserClient} to get the transaction id as well as IP address
 * and port number of the primary {@link TransactionManager}
 * 
 */
public class TransactionManagerRequest extends Message {

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "transaction_manager_request";

    @JSONAnnotation(key = JSONCommunication.KEY_FOR_HASH)
    private int localityHash;

    @JSONConstructor
    public TransactionManagerRequest(int hash) {
        this.localityHash = hash;
    }

    @Override
    public String toString() {
        return MESSAGE_TYPE + " : hash=" + localityHash;
    }

    public int getLocalityHash() {
        return localityHash;
    }
}
