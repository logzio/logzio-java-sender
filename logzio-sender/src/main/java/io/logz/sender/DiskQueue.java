package io.logz.sender;

import com.bluejeans.common.bigqueue.BigQueue;
import io.logz.sender.exceptions.LogzioParameterErrorException;

import java.io.File;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DiskQueue implements LogsQueue {
    private final BigQueue logsQueue;
    private final File queueDirectory;
    private final boolean dontCheckEnoughDiskSpace;
    private final int fsPercentThreshold;
    private final SenderStatusReporter reporter;
    private volatile boolean isEnoughSpace;

    private DiskQueue(File queueDir, boolean dontCheckEnoughDiskSpace, int fsPercentThreshold,
                      int gcPersistedQueueFilesIntervalSeconds, SenderStatusReporter reporter,
                      int checkDiskSpaceInterval, ScheduledExecutorService diskSpaceTasks)
            throws LogzioParameterErrorException {

        this.reporter = reporter;
        queueDirectory = queueDir;
        validateParameters();
        // divide bufferDir to dir and queue name
        String dir = queueDir.getAbsoluteFile().getParent();
        String queueNameDir = queueDir.getName();
        if (dir == null || queueNameDir.isEmpty() ) {
            throw new LogzioParameterErrorException("queueDir", " value is empty: " + queueDir.getAbsolutePath());
        }
        logsQueue = new BigQueue(dir, queueNameDir);
        this.dontCheckEnoughDiskSpace = dontCheckEnoughDiskSpace;
        this.fsPercentThreshold = fsPercentThreshold;
        this.isEnoughSpace = true;
        diskSpaceTasks.scheduleWithFixedDelay(this::gcBigQueue, 0, gcPersistedQueueFilesIntervalSeconds, TimeUnit.SECONDS);
        diskSpaceTasks.scheduleWithFixedDelay(this::validateEnoughSpace, 0, checkDiskSpaceInterval, TimeUnit.MILLISECONDS);
    }

    private void validateParameters() throws LogzioParameterErrorException {
        if (queueDirectory == null) {
            throw new LogzioParameterErrorException("bufferDir", "value is null.");
        }
        if (reporter == null) {
            throw new LogzioParameterErrorException("reporter", "value is null.");
        }
    }

    @Override
    public void enqueue(byte[] log) {
        if (isEnoughSpace) {
            logsQueue.enqueue(log);
        }
    }

    @Override
    public byte[] dequeue() {
        return logsQueue.dequeue();
    }

    @Override
    public boolean isEmpty() {
        return logsQueue.isEmpty();
    }

    private void validateEnoughSpace() {
        try {
            if (dontCheckEnoughDiskSpace) {
                return;
            }
            int actualUsedFsPercent = 100 - ((int) (((double) queueDirectory.getUsableSpace() / queueDirectory.getTotalSpace()) * 100));
            if (actualUsedFsPercent >= fsPercentThreshold) {
                if (isEnoughSpace) {
                    reporter.warning(String.format("Logz.io: Dropping logs, as FS used space on %s is %d percent, and the drop threshold is %d percent",
                            queueDirectory.getAbsolutePath(), actualUsedFsPercent, fsPercentThreshold));
                }
                isEnoughSpace = false;
            } else {
                isEnoughSpace = true;
            }
        } catch (Throwable e) {
            reporter.error("Uncaught error from validateEnoughSpace()", e);
        }
    }

    private void gcBigQueue() {
        try {
            logsQueue.gc();
        } catch (Throwable e) {
            // We cant throw anything out, or the task will stop, so just swallow all
            reporter.error("Uncaught error from BigQueue.gc()", e);
        }
    }

    @Override
    public void close() {
        gcBigQueue();
    }

    public static class Builder {
        private boolean dontCheckEnoughDiskSpace = false;
        private int fsPercentThreshold = 98;
        private int gcPersistedQueueFilesIntervalSeconds = 30;
        private int checkDiskSpaceInterval = 1000;
        private File setQueueDir;
        private SenderStatusReporter reporter;
        private ScheduledExecutorService diskSpaceTasks;
        private LogzioSender.Builder context;

        Builder(LogzioSender.Builder context, ScheduledExecutorService diskSpaceTasks) {
            this.context = context;
            this.diskSpaceTasks = diskSpaceTasks;
        }

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

        public Builder setCheckDiskSpaceInterval(int checkDiskSpaceInterval) {
            this.checkDiskSpaceInterval = checkDiskSpaceInterval;
            return this;
        }

        public Builder setQueueDir(File setQueueDir) {
            this.setQueueDir = setQueueDir;
            return this;
        }

        Builder setReporter(SenderStatusReporter reporter) {
            this.reporter = reporter;
            return this;
        }

        Builder setDiskSpaceTasks(ScheduledExecutorService diskSpaceTasks) {
            this.diskSpaceTasks = diskSpaceTasks;
            return this;
        }

        public LogzioSender.Builder endDiskQueue() {
            context.setDiskQueueBuilder(this);
            return context;
        }

        DiskQueue build() throws LogzioParameterErrorException {
            return new DiskQueue(setQueueDir, dontCheckEnoughDiskSpace, fsPercentThreshold,
                    gcPersistedQueueFilesIntervalSeconds, reporter, checkDiskSpaceInterval, diskSpaceTasks);
        }
    }

    public static Builder builder(LogzioSender.Builder context, ScheduledExecutorService diskSpaceTasks){
        return new Builder(context, diskSpaceTasks);
    }
}
