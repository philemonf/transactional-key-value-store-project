package ch.epfl.tkvs.test.userclient;

import ch.epfl.tkvs.exceptions.AbortException;
import ch.epfl.tkvs.user.UserTransaction;


public class UserClient implements Runnable {

    @Override
    public void run() {
        try {

            System.out.println("User Client starting");
            final MyKey k0 = new MyKey("k0", 0);
            final MyKey k1 = new MyKey("k1", 1);

            UserTransaction t0 = new UserTransaction<MyKey>();
            t0.begin(k0);
            t0.write(k0, "k0_main");
            t0.commit();

            Thread thread1 = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        UserTransaction<MyKey> t = new UserTransaction<MyKey>();
                        t.begin(k0);

                        t.write(k0, "k0_thread1");
                        System.out.println((String) t.read(k0));
                        t.commit();
                    } catch (AbortException e) {
                        System.err.println("thread1 aborted. Restarting.");
                        // run();
                    }
                }
            });

            Thread thread2 = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        UserTransaction<MyKey> t = new UserTransaction<MyKey>();
                        t.begin(k1);

                        t.write(k1, "myValue");
                        System.out.println((String) t.read(k0));
                        System.out.println((String) t.read(k1));
                        t.commit();
                    } catch (AbortException e) {
                        System.err.println("thread2 aborted. Restarting.");
                        // run();
                    }
                }
            });

            thread1.start();
            thread2.start();

            thread1.join();
            thread2.join();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
