package ch.epfl.tkvs.test.userclient;

import ch.epfl.tkvs.yarn.appmaster.AppMaster;


public class UserClient implements Runnable {

    @Override
    public void run() {
        try {
            System.out.println("User Client starting");
            Transaction.initialize("localhost", AppMaster.port);
            MyKey k0 = new MyKey("myKey");
            Transaction t = new Transaction(k0);
            t.write(k0, "myValue".getBytes());
            System.out.println(new String(t.read(k0)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
