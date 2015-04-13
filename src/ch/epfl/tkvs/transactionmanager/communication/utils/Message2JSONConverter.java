package ch.epfl.tkvs.transactionmanager.communication.utils;

import java.lang.reflect.Field;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ch.epfl.tkvs.transactionmanager.communication.JSONAnnotation;
import ch.epfl.tkvs.transactionmanager.communication.Message;


public class Message2JSONConverter {

    public static JSONObject toJSON(Message message) throws JSONException {
        JSONObject json = new JSONObject();

        Class<? extends Message> messageClass = message.getClass();

        Field[] fields = messageClass.getDeclaredFields();
        for (Field field : fields) {

            JSONAnnotation jsonAnnot = (JSONAnnotation) field.getAnnotation(JSONAnnotation.class);

            if (jsonAnnot != null) {
                boolean isAccessible = field.isAccessible();
                field.setAccessible(true);

                try {
                    json.put(jsonAnnot.key(), field.get(message));
                } catch (IllegalAccessException e) {
                    throw new JSONException(e);
                }

                field.setAccessible(isAccessible);
            }
        }

        return json;
    }
}
