package io.logz.sender;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

public class InMemoryQueue implements LogsQueue {
    private static final int MB_IN_BYTES = 1024 * 1024;
    public static int DONT_LIMIT_BUFFER_SPACE = -1;
    private final ConcurrentLinkedQueue<byte[]> logsBuffer;
    private final boolean dontCheckEnoughMemorySpace;
    private final long inMemoryQueueCapacityInBytes;
    private final SenderStatusReporter reporter;
    private long size;
    private final ReentrantLock queueLock;

    private InMemoryQueue(boolean dontCheckEnoughMemorySpace, long inMemoryQueueCapacityInBytes, SenderStatusReporter reporter) {
        logsBuffer = new ConcurrentLinkedQueue<>();
        this.dontCheckEnoughMemorySpace = dontCheckEnoughMemorySpace;
        this.inMemoryQueueCapacityInBytes = inMemoryQueueCapacityInBytes;
        this.reporter = reporter;
        this.size = 0;
        this.queueLock = new ReentrantLock();
    }

    @Override
    public void enqueue(byte[] log) {
        queueLock.lock();
        try {
            if(isEnoughSpace()) {
                logsBuffer.add(log);
                size += log.length;
            }
        }finally {
            queueLock.unlock();
        }
    }

    @Override
    public byte[] dequeue() {
        queueLock.lock();
        byte[] log;
        try {
            log = logsBuffer.remove();
            size -= log.length;
        }finally {
            queueLock.unlock();
        }
        return log;
    }

    @Override
    public boolean isEmpty() {
        return logsBuffer.isEmpty();
    }

    private boolean isEnoughSpace() {
        if (dontCheckEnoughMemorySpace) {
            return true;
        }

        if (size >= inMemoryQueueCapacityInBytes) {
            reporter.warning(String.format("Logz.io: Dropping logs - we crossed the memory threshold of %d MB",
                    inMemoryQueueCapacityInBytes/(MB_IN_BYTES)));
            return false;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
    }

    public static class Builder {
        private long inMemoryQueueCapacityInBytes = MB_IN_BYTES * 100; //100MB memory limit
        private SenderStatusReporter reporter;
        private boolean dontCheckEnoughMemorySpace = false;
        private LogzioSender.Builder context;

        Builder(LogzioSender.Builder context) {
            this.context = context;
        }

        public Builder setInMemoryQueueCapacityInBytes(long inMemoryQueueCapacityInBytes) {
            this.inMemoryQueueCapacityInBytes = inMemoryQueueCapacityInBytes;
            if (inMemoryQueueCapacityInBytes == DONT_LIMIT_BUFFER_SPACE) {
                dontCheckEnoughMemorySpace = true;
            }
            return this;
        }

        Builder setReporter(SenderStatusReporter reporter) {
            this.reporter = reporter;
            return this;
        }

        public LogzioSender.Builder endInMemoryQueue() {
            context.setInMemoryQueueBuilder(this);
            return context;
        }

        public InMemoryQueue build() {
            return new InMemoryQueue(dontCheckEnoughMemorySpace, inMemoryQueueCapacityInBytes, reporter);
        }
    }

    public static Builder builder(LogzioSender.Builder context){
        return new Builder(context);
    }
}
