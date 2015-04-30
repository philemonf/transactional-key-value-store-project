package ch.epfl.tkvs.yarn;

import java.io.Serializable;
import java.util.HashMap;


public class RoutingTable implements Serializable {

    private final static long serialVersionUID = 1;

    private final String AM_IP;
    private final int AM_PORT;
    private HashMap<String, Integer> tms;

    public RoutingTable(String AM_IP, int AM_PORT) {
        this.AM_IP = AM_IP;
        this.AM_PORT = AM_PORT;
        tms = new HashMap<String, Integer>();
    }

    public void addTM(String ip, int port) {
        tms.put(ip, port);
    }

    public HashMap<String, Integer> getTMs() {
        return tms;
    }

    public String getAMIp() {
        return AM_IP;
    }

    public int getAMPort() {
        return AM_PORT;
    }

    public int size() {
        return tms.size();
    }

    public boolean contains(String tmIp) {
        return tms.containsKey(tmIp);
    }
}
