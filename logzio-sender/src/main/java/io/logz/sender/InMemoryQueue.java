package io.logz.sender;

import io.logz.sender.exceptions.LogzioParameterErrorException;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryQueue implements LogsQueue {
    private static final int MG_IN_BYTES = 1024 * 1024;
    public static int DONT_LIMIT_BUFFER_SPACE = -1;
    private final ConcurrentLinkedQueue<byte[]> logsBuffer;
    private final boolean dontCheckEnoughMemorySpace;
    private final int capacityInBytes;
    private final SenderStatusReporter reporter;
    private AtomicInteger size;

    private InMemoryQueue(boolean dontCheckEnoughMemorySpace, int capacityInBytes, SenderStatusReporter reporter) {
        logsBuffer = new ConcurrentLinkedQueue<>();
        this.dontCheckEnoughMemorySpace = dontCheckEnoughMemorySpace;
        this.capacityInBytes = capacityInBytes;
        this.reporter = reporter;
        this.size = new AtomicInteger();
    }

    @Override
    public void enqueue(byte[] log) {
        if(isEnoughSpace()) {
            logsBuffer.add(log);
            size.getAndAdd(log.length);
        }
    }

    @Override
    public byte[] dequeue() {
        byte[] log =  logsBuffer.remove();
        size.getAndAdd(-log.length);
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

        if (size.get() >= capacityInBytes ) {
            reporter.warning(String.format("Logz.io: Dropping logs - we crossed the memory threshold of %d MB",
                    capacityInBytes/(MG_IN_BYTES)));
            return false;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
    }

    public static class Builder {
        private int capacityInBytes = MG_IN_BYTES * 100; //100MB memory limit
        private SenderStatusReporter reporter;
        private boolean dontCheckEnoughMemorySpace = false;
        private LogzioSender.Builder context;

        Builder(LogzioSender.Builder context) {
            this.context = context;
        }

        public Builder setCapacityInBytes(int capacityInBytes) {
            this.capacityInBytes = capacityInBytes;
            if (capacityInBytes == DONT_LIMIT_BUFFER_SPACE) {
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

        public InMemoryQueue build() throws LogzioParameterErrorException{
            return new InMemoryQueue(dontCheckEnoughMemorySpace, capacityInBytes, reporter);
        }
    }

    public static Builder builder(LogzioSender.Builder context){
        return new Builder(context);
    }
}
