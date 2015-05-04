package ch.epfl.tkvs.test.userclient;

import ch.epfl.tkvs.user.UserTransaction;


public class UserClient implements Runnable {

    @Override
    public void run() {
        try {
            System.out.println("User Client starting");
            MyKey k0 = new MyKey("myKey");
            UserTransaction t = new UserTransaction<MyKey>(k0);
            t.write(k0, "myValue");
            System.out.println((String) t.read(k0));
            t.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
