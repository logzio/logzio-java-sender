package io.logz.sender;


import java.io.Closeable;

public interface LogzioLogsBufferInterface extends Closeable{
    void enqueue(byte[] log);
    byte[] dequeue();
    boolean isEmpty();
}