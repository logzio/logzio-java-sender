package io.logz.sender;


import java.io.Closeable;

interface LogsQueue extends Closeable {
    void enqueue(byte[] log);
    byte[] dequeue();
    boolean isEmpty();
}