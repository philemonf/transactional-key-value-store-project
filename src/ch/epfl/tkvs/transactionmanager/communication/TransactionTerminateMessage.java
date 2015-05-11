package ch.epfl.tkvs.transactionmanager.communication;

import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_MESSAGE_TYPE;
import static ch.epfl.tkvs.transactionmanager.communication.JSONCommunication.KEY_FOR_TRANSACTION_ID;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import ch.epfl.tkvs.transactionmanager.TransactionManager;
import ch.epfl.tkvs.transactionmanager.algorithms.MVTO;
import ch.epfl.tkvs.transactionmanager.communication.responses.MinAliveTransactionResponse;
import ch.epfl.tkvs.transactionmanager.communication.utils.Base64Utils;
import ch.epfl.tkvs.yarn.appmaster.AppMaster;


/**
 * This message is used in the garbage collection process of {@link MVTO}. It is sent by a {@link TransactionManager} to
 * the {@link AppMaster} to inform the later of the transaction that have terminated (commit or aborted).
 * 
 * In returns, the {@link MVTO}'s garbage collector waits for a {@link MinAliveTransactionResponse}.
 * 
 * This outlined process occurs in the checkpoint method of {@link MVTO}.
 */
public class TransactionTerminateMessage extends Message {

    @JSONAnnotation(key = KEY_FOR_MESSAGE_TYPE)
    public static final String MESSAGE_TYPE = "xact_terminate_message";

    @JSONAnnotation(key = KEY_FOR_TRANSACTION_ID)
    private String encodedTids;

    @JSONConstructor
    public TransactionTerminateMessage(LinkedList<Integer> tids) throws IOException {

        encodedTids = Base64Utils.convertToBase64(tids);
    }

    public TransactionTerminateMessage(int transactionId) throws IOException {
        this(new LinkedList<Integer>(Arrays.asList(transactionId)));
    }

    @SuppressWarnings("unchecked")
    public LinkedList<Integer> getTransactionIds() {
        try {
            return (LinkedList<Integer>) Base64Utils.convertFromBase64(encodedTids);
        } catch (Exception e) {
            return new LinkedList<Integer>();
        }
    }
}
