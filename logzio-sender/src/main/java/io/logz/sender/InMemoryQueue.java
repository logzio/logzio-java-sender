package io.logz.sender;

import io.logz.sender.exceptions.LogzioParameterErrorException;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryQueue implements LogzioLogsBufferInterface{
    private final ConcurrentLinkedQueue<byte[]> logsBuffer;
    private final boolean dontCheckEnoughMemorySpace;
    private final int bufferThreshold;
    private final SenderStatusReporter reporter;
    private AtomicInteger size;

    private InMemoryQueue(boolean dontCheckEnoughMemorySpace, int bufferThreshold, SenderStatusReporter reporter)
            throws LogzioParameterErrorException {

        logsBuffer = new ConcurrentLinkedQueue<>();
        this.dontCheckEnoughMemorySpace = dontCheckEnoughMemorySpace;
        this.bufferThreshold = bufferThreshold;
        this.reporter = reporter;
        this.size = new AtomicInteger();
        validateParameters();
    }

    private void validateParameters() throws LogzioParameterErrorException {
        if (reporter == null) {
            throw new LogzioParameterErrorException("reporter", "value is null.");
        }
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
        size.getAndAdd(-log.length);
        return log;
    }

    @Override
    public boolean isEmpty() {
        return logsBuffer.isEmpty();
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
        private LogzioSender.Builder context;

        Builder(LogzioSender.Builder context) {
            this.context = context;
        }

        public Builder setBufferThreshold(int bufferThreshold) {
            this.bufferThreshold = bufferThreshold;
            if (bufferThreshold == -1) {
                dontCheckEnoughMemorySpace = true;
            }
            return this;
        }

        Builder setReporter(SenderStatusReporter reporter) {
            this.reporter = reporter;
            return this;
        }

        public LogzioSender.Builder EndInMemoryQueue() {
            context.setInMemoryQueueBuilder(this);
            return context;
        }

        public InMemoryQueue build() throws LogzioParameterErrorException{
            return new InMemoryQueue(dontCheckEnoughMemorySpace, bufferThreshold, reporter);
        }
    }

    public static Builder builder(LogzioSender.Builder context){
        return new Builder(context);
    }
}
