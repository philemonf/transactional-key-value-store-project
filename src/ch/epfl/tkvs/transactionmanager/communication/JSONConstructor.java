package ch.epfl.tkvs.transactionmanager.communication;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


/**
 * Indicates which constructor the JSON decoding process should use.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface JSONConstructor {

}
