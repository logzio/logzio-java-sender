package io.logz.sender;


import java.io.Closeable;
import java.io.IOException;

public interface LogsQueue extends Closeable {
    void enqueue(byte[] log);
    byte[] dequeue();
    boolean isEmpty();
    void clear() throws IOException;
}