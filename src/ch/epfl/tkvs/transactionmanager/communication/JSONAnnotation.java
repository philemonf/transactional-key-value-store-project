package ch.epfl.tkvs.transactionmanager.communication;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface JSONAnnotation {
	String key();
}