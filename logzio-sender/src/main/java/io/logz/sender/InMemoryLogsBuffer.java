package io.logz.sender;

import io.logz.sender.exceptions.LogzioParameterErrorException;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryLogsBuffer implements LogzioLogsBufferInterface {
    private static final int ONE_MEGA_BYTE_IN_BYTES = 1024 * 1024;
    public static int DONT_LIMIT_BUFFER_SPACE = -1;
    private final ConcurrentLinkedQueue<byte[]> logsBuffer;
    private final boolean dontCheckEnoughMemorySpace;
    private final int bufferThreshold;
    private final SenderStatusReporter reporter;
    private AtomicInteger size;

    private InMemoryLogsBuffer(boolean dontCheckEnoughMemorySpace, int bufferThreshold, SenderStatusReporter reporter)
            throws LogzioParameterErrorException {

        logsBuffer = new ConcurrentLinkedQueue<>();
        this.dontCheckEnoughMemorySpace = dontCheckEnoughMemorySpace;
        this.bufferThreshold = bufferThreshold;
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

        if (size.get() >= bufferThreshold ) {
            reporter.warning(String.format("Logz.io: Dropping logs - we crossed the memory threshold of %d MB",
                    bufferThreshold/(ONE_MEGA_BYTE_IN_BYTES)));
            return false;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
    }

    public static class Builder {
        private int bufferThreshold = ONE_MEGA_BYTE_IN_BYTES * 100; //100MB memory limit
        private SenderStatusReporter reporter;
        private boolean dontCheckEnoughMemorySpace = false;
        private LogzioSender.Builder context;

        Builder(LogzioSender.Builder context) {
            this.context = context;
        }

        public Builder setBufferThreshold(int bufferThreshold) {
            this.bufferThreshold = bufferThreshold;
            if (bufferThreshold == DONT_LIMIT_BUFFER_SPACE) {
                dontCheckEnoughMemorySpace = true;
            }
            return this;
        }

        Builder setReporter(SenderStatusReporter reporter) {
            this.reporter = reporter;
            return this;
        }

        public LogzioSender.Builder EndInMemoryLogsBuffer() {
            context.setInMemoryLogsBufferBuilder(this);
            return context;
        }

        public InMemoryLogsBuffer build() throws LogzioParameterErrorException{
            return new InMemoryLogsBuffer(dontCheckEnoughMemorySpace, bufferThreshold, reporter);
        }
    }

    public static Builder builder(LogzioSender.Builder context){
        return new Builder(context);
    }
}
