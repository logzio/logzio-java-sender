package io.logz.sender;


import java.io.Closeable;

public interface LogsQueue extends Closeable{
    void enqueue(byte[] log);
    byte[] dequeue();
    boolean isEmpty();
}