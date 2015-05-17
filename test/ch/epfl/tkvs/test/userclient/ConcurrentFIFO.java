package ch.epfl.tkvs.test.userclient;

public class ConcurrentFIFO {

    private double buffer[];
    private int currentIndex;
    private int maxSize;

    public ConcurrentFIFO(int maxSize) {
        this.buffer = new double[maxSize];
        this.currentIndex = 0;
        this.maxSize = maxSize;
    }

    public synchronized void add(double d) {
        buffer[currentIndex] = d;
        currentIndex++;

        if (currentIndex >= maxSize) {
            currentIndex = 0;
        }
    }

    public synchronized double get(int i) {
        return buffer[i];
    }

    public synchronized int size() {
        return maxSize;
    }
}
