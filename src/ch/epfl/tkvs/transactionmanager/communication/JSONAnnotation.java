package ch.epfl.tkvs.transactionmanager.communication;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


/**
 * This annotation is used to specify the JSON key that one wants to the attribute of a {@link Message}.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface JSONAnnotation {

    String key();
}
