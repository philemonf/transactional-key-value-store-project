package ch.epfl.tkvs.transactionmanager.communication.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.JSONConstructor;
import ch.epfl.tkvs.transactionmanager.communication.Message;


public class JSON2MessageConverter {

    public static class InvalidMessageException extends Exception {

        private static final long serialVersionUID = 7037465155158262924L;

        public InvalidMessageException(Exception e) {
            super(e);
        }

        public InvalidMessageException(String m) {
            super(m);
        }
    }

    public static Message parseJSON(JSONObject json, Class<? extends Message> messageClass) throws InvalidMessageException {
        if (json == null) {
            throw new InvalidMessageException("json is null.");
        }

        if (messageClass == null) {
            throw new InvalidMessageException("messageClass is null.");
        }

        try {
            Constructor<?>[] constructors = messageClass.getConstructors();
            
            if (constructors.length == 0) {
                throw new InvalidMessageException(messageClass + " has no public constructor.");
            }
            Constructor<? extends Message> constructor = null;
            
            for (Constructor<?> candidateConstructor : constructors) {
            	if (candidateConstructor.isAnnotationPresent(JSONConstructor.class)) {
            		System.err.println("ANNOTATION FOUND: " + candidateConstructor);
            		constructor = (Constructor<? extends Message>)candidateConstructor;
            		break;
            	}
            }
            
            if (constructor == null) {
            	constructor = (Constructor<? extends Message>) constructors[0];
            }
            
            //
            // System.err.println("Constructor for " + messageClass);
            // for (Class c : constructor.getParameterTypes())
            // System.err.println("*" + c);

            List<Object> dummyParams = new LinkedList<Object>();

            for (Class parameterType : constructor.getParameterTypes()) {
                if (parameterType.isPrimitive()) {
                    dummyParams.add(parameterType == boolean.class ? false : 0);
                } else {
                    dummyParams.add(null);
                }
            }

            // System.err.println("Dummy object" + dummyParams);
            // System.err.println("json object" + json);

            Message message = constructor.newInstance(dummyParams.toArray());

            Field[] fields = messageClass.getDeclaredFields();
            for (Field field : fields) {
                JSONAnnotation jsonAnnot = (JSONAnnotation) field.getAnnotation(JSONAnnotation.class);

                if (jsonAnnot != null) {

                    Object value = json.get(jsonAnnot.key());
                    Method getMethod = null;
                    if (field.getType().isPrimitive() && !value.getClass().isPrimitive()) {
                        String methodName = field.getType().getSimpleName() + "Value";

                        try {
                            getMethod = value.getClass().getMethod(methodName);
                        } catch (NoSuchMethodException e) {
                            throw new InvalidMessageException(value.getClass() + " had no method named " + methodName);
                        }

                    }

                    if (field.getType().isPrimitive() || field.getType().isAssignableFrom(value.getClass())) {
                        if (!Modifier.isFinal(field.getModifiers())) {
                            boolean isFieldAccessible = field.isAccessible();
                            field.setAccessible(true);
                            field.set(message, (getMethod == null) ? value : getMethod.invoke(value));
                            field.setAccessible(isFieldAccessible);
                        }
                    } else {

                        throw new InvalidMessageException(jsonAnnot.key() + " has type " + value.getClass());
                    }
                }

            }

            return message;

        } catch (JSONException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            e.printStackTrace();
            throw new InvalidMessageException(e);
        }
    }
}
