package ch.epfl.tkvs.transactionmanager.communication.utils;

import org.apache.commons.codec.binary.Base64;

import java.io.*;


public class Base64Utils {

    public static Serializable convertFromBase64(String base64) throws IOException, ClassNotFoundException {
        byte[] buf = Base64.decodeBase64(base64);
        ByteArrayInputStream bis = new ByteArrayInputStream(buf);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (Serializable) ois.readObject();
    }

    public static String convertToBase64(Serializable data) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);

        oos.writeObject(data);

        byte[] bytes = bos.toByteArray();
        bos.close();

        return Base64.encodeBase64String(bytes).replaceAll("[\r\n]+", "");
    }
}
