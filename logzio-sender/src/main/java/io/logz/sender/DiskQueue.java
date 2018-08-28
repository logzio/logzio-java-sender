package io.logz.sender;

import com.bluejeans.common.bigqueue.BigQueue;
import io.logz.sender.exceptions.LogzioParameterErrorException;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DiskQueue implements LogzioLogsBufferInterface{
    private final BigQueue logsBuffer;
    private final File queueDirectory;
    private final boolean dontCheckEnoughDiskSpace;
    private final int fsPercentThreshold;
    private final SenderStatusReporter reporter;
    private final ScheduledExecutorService cleanDiskSpaceService;

    private DiskQueue(File bufferDir, boolean dontCheckEnoughDiskSpace, int fsPercentThreshold,
                      int gcPersistedQueueFilesIntervalSeconds, SenderStatusReporter reporter) throws LogzioParameterErrorException {
        // divide bufferDir to dir and queue name
        if (bufferDir == null) {
            throw new LogzioParameterErrorException("bufferDir", "value is null.");
        }
        String dir = bufferDir.getAbsoluteFile().getParent();
        String queueNameDir = bufferDir.getName();
        if (dir == null || queueNameDir.isEmpty() ) {
            throw new LogzioParameterErrorException("bufferDir", " value is empty: "+bufferDir.getAbsolutePath());
        }
        logsBuffer = new BigQueue(dir, queueNameDir);
        queueDirectory = bufferDir;
        this.dontCheckEnoughDiskSpace = dontCheckEnoughDiskSpace;
        this.fsPercentThreshold = fsPercentThreshold;
        this.reporter = reporter;
        this.cleanDiskSpaceService =  Executors.newSingleThreadScheduledExecutor();
        cleanDiskSpaceService.scheduleWithFixedDelay(this::gcBigQueue, 0, gcPersistedQueueFilesIntervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void enqueue(byte[] log) {
        if (isEnoughSpace()) {
            logsBuffer.enqueue(log);
        }
    }

    @Override
    public byte[] dequeue() {
        return logsBuffer.dequeue();
    }

    @Override
    public boolean isEmpty() {
        return logsBuffer.isEmpty();
    }

    private boolean isEnoughSpace() {
        if (dontCheckEnoughDiskSpace) {
            return true;
        }
        int actualUsedFsPercent = 100 - ((int) (((double) queueDirectory.getUsableSpace() / queueDirectory.getTotalSpace()) * 100));
        if (actualUsedFsPercent >= fsPercentThreshold) {
            reporter.warning(String.format("Logz.io: Dropping logs, as FS used space on %s is %d percent, and the drop threshold is %d percent",
                    queueDirectory.getAbsolutePath(), actualUsedFsPercent, fsPercentThreshold));
            return false;
        } else {
            return true;
        }
    }

    private void gcBigQueue() {
        try {
            logsBuffer.gc();
        } catch (Throwable e) {
            // We cant throw anything out, or the task will stop, so just swallow all
            reporter.error("Uncaught error from BigQueue.gc()", e);
        }
    }

    @Override
    public void close() {
        gcBigQueue();
        cleanDiskSpaceService.shutdownNow();
    }

    public static class Builder {
        private boolean dontCheckEnoughDiskSpace = false;
        private int fsPercentThreshold = 98;
        private int gcPersistedQueueFilesIntervalSeconds = 30;
        private File bufferDir;
        private SenderStatusReporter reporter;

        public Builder setFsPercentThreshold(int fsPercentThreshold) {
            this.fsPercentThreshold = fsPercentThreshold;
            if (fsPercentThreshold == -1) {
                dontCheckEnoughDiskSpace = true;
            }
            return this;
        }

        public Builder setGcPersistedQueueFilesIntervalSeconds(int gcPersistedQueueFilesIntervalSeconds) {
            this.gcPersistedQueueFilesIntervalSeconds = gcPersistedQueueFilesIntervalSeconds;
            return this;
        }

        public Builder setBufferDir(File bufferDir) {
            this.bufferDir = bufferDir;
            return this;
        }

        public Builder setReporter(SenderStatusReporter reporter) {
            this.reporter = reporter;
            return this;
        }

        public DiskQueue build() throws LogzioParameterErrorException {
            return new DiskQueue(bufferDir, dontCheckEnoughDiskSpace, fsPercentThreshold, gcPersistedQueueFilesIntervalSeconds, reporter);
        }
    }

    public static Builder builder(){
        return new Builder();
    }
}
