package io.logz.sender;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryQueue implements LogzioLogsBufferInterface{
    private final ConcurrentLinkedQueue<byte[]> logsBuffer;
    private final boolean dontCheckEnoughMemorySpace;
    private final int bufferThreshold;
    private final SenderStatusReporter reporter;
    private AtomicInteger size;

    private InMemoryQueue(boolean dontCheckEnoughMemorySpace, int bufferThreshold, SenderStatusReporter reporter) {
        logsBuffer = new ConcurrentLinkedQueue<>();
        this.dontCheckEnoughMemorySpace = dontCheckEnoughMemorySpace;
        this.bufferThreshold = bufferThreshold;
        this.reporter = reporter;
        this.size = new AtomicInteger();
    }

    @Override
    public void enqueue(byte[] log) {
        if(isEnoughsSpace()) {
            logsBuffer.add(log);
            size.getAndAdd(log.length);
        }

    }

    @Override
    public byte[] dequeue() {
        byte[] log =  logsBuffer.remove();
        size.getAndAdd(-logsBuffer.size());
        return log;
    }

    @Override
    public boolean isEmpty() {
        return logsBuffer.size() == 0;
    }

    private boolean isEnoughsSpace() {
        if (dontCheckEnoughMemorySpace) {
            return true;
        }

        if (size.get() >= bufferThreshold ) {
            reporter.warning(String.format("Logz.io: Dropping logs - we crossed the memory threshold of %d MB",
                    bufferThreshold/(1024 * 1024)));
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void close() throws IOException {
    }

    public static class Builder {
        private int bufferThreshold = 1024 * 1024 * 100; //100MB memory limit
        private SenderStatusReporter reporter;
        private boolean dontCheckEnoughMemorySpace = false;

        public Builder setBufferThreshold(int bufferThreshold) {
            this.bufferThreshold = bufferThreshold;
            return this;
        }

        public Builder setReporter(SenderStatusReporter reporter) {
            this.reporter = reporter;
            return this;
        }

        public Builder setDontCheckEnoughMemorySpace(boolean check) {
            this.dontCheckEnoughMemorySpace = check;
            return this;
        }

        public InMemoryQueue build() {
            return new InMemoryQueue(dontCheckEnoughMemorySpace, bufferThreshold, reporter);
        }
    }

    public static Builder builder(){
        return new Builder();
    }
}
