package io.logz.sender;

import com.bluejeans.common.bigqueue.BigQueue;
import com.google.gson.JsonObject;
import io.logz.sender.exceptions.LogzioParameterErrorException;
import io.logz.sender.exceptions.LogzioServerErrorException;

import java.io.File;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class LogzioSender {
    private static final int MAX_SIZE_IN_BYTES = 3 * 1024 * 1024;  // 3 MB

    private static final Map<String, LogzioSender> logzioSenderInstances = new HashMap<>();
    private static final int FINAL_DRAIN_TIMEOUT_SEC = 20;

    private final LogzioLogsBufferInterface logsBuffer;
    private final int drainTimeout;
    private final boolean debug;
    private final SenderStatusReporter reporter;
    private ScheduledExecutorService tasksExecutor;
    private final AtomicBoolean drainRunning = new AtomicBoolean(false);
    private final HttpsSyncSender httpsSyncSender;


    private LogzioSender(String logzioToken, String logzioType, int drainTimeout, int fsPercentThreshold, File bufferDir,
                         String logzioUrl, int socketTimeout, int connectTimeout, boolean debug,
                         SenderStatusReporter reporter, ScheduledExecutorService tasksExecutor,
                         int gcPersistedQueueFilesIntervalSeconds, boolean compressRequests, LogzioLogsBufferInterface logsBuffer,
                         int bufferThreshold)
            throws  LogzioParameterErrorException {

        HttpsRequestConfiguration httpsRequestConfiguration = HttpsRequestConfiguration
                .builder()
                .setLogzioToken(logzioToken)
                .setLogzioType(logzioType)
                .setLogzioListenerUrl(logzioUrl)
                .setSocketTimeout(socketTimeout)
                .setConnectTimeout(connectTimeout)
                .setCompressRequests(compressRequests)
                .build();
        this.logsBuffer = logsBuffer;

        this.drainTimeout = drainTimeout;
        this.debug = debug;
        this.reporter = reporter;
        httpsSyncSender = new HttpsSyncSender(httpsRequestConfiguration, reporter);
        //todo check thread handling gc
        this.tasksExecutor = tasksExecutor;

        debug("Created new LogzioSender class");
    }

    @Deprecated
    public static synchronized LogzioSender getOrCreateSenderByType(String logzioToken, String logzioType, int drainTimeout, int fsPercentThreshold, File bufferDir,
                                                                    String logzioUrl, int socketTimeout, int connectTimeout, boolean debug,
                                                                    SenderStatusReporter reporter, ScheduledExecutorService tasksExecutor,
                                                                    int gcPersistedQueueFilesIntervalSeconds, boolean compressRequests) throws LogzioParameterErrorException {
        return getLogzioSender(logzioToken, logzioType, drainTimeout, fsPercentThreshold, bufferDir, logzioUrl, socketTimeout, connectTimeout, debug, reporter, tasksExecutor, gcPersistedQueueFilesIntervalSeconds, compressRequests, null, 0);


    }

    private static LogzioSender getLogzioSender(String logzioToken, String logzioType,
                                                int drainTimeout, int fsPercentThreshold, File bufferDir,
                                                String logzioUrl, int socketTimeout, int connectTimeout, boolean debug,
                                                SenderStatusReporter reporter, ScheduledExecutorService tasksExecutor,
                                                int gcPersistedQueueFilesIntervalSeconds, boolean compressRequests,
                                                LogzioLogsBufferInterface buffer, int bufferThreshold) throws LogzioParameterErrorException {
        // We want one buffer per logzio data type.
        // so that's why I create separate buffers per type.
        // BUT - users not always understand the notion of types at first, and can define multiple data sender on the same type - and this is what I want to protect by this factory.
        LogzioSender logzioSenderInstance = logzioSenderInstances.get(logzioType);
        if (logzioSenderInstance == null) {
            if (bufferDir == null) {
                throw new LogzioParameterErrorException("bufferDir", "null");
            }

            LogzioSender logzioSender = new LogzioSender(logzioToken, logzioType, drainTimeout, fsPercentThreshold,
                    bufferDir, logzioUrl, socketTimeout, connectTimeout, debug, reporter,
                    tasksExecutor, gcPersistedQueueFilesIntervalSeconds, compressRequests,
                    buffer == null ?
                            getDefaultQueue(bufferDir, fsPercentThreshold, reporter, gcPersistedQueueFilesIntervalSeconds)
                            : buffer,
                    bufferThreshold);
            logzioSenderInstances.put(logzioType, logzioSender);
            return logzioSender;
        } else {
            reporter.info("Already found appender configured for type " + logzioType + ", re-using the same one.");

            // Sometimes (For example under Spring) the framework closes logback entirely (thos closing the executor)
            // So we need to take a new one instead, as we can grantee that nothing is running now because it is terminated.
            if (logzioSenderInstance.tasksExecutor.isTerminated()) {
                reporter.info("The old task executor is terminated! replacing it with a new one");
                logzioSenderInstance.tasksExecutor = tasksExecutor;
            }
            return logzioSenderInstance;
        }
    }

    @Deprecated
    public static synchronized LogzioSender getOrCreateSenderByType(String logzioToken, String logzioType, int drainTimeout, int fsPercentThreshold, File bufferDir,
                                                                    String logzioUrl, int socketTimeout, int connectTimeout, boolean debug,
                                                                    SenderStatusReporter reporter, ScheduledExecutorService tasksExecutor,
                                                                    int gcPersistedQueueFilesIntervalSeconds) throws LogzioParameterErrorException {
        return getOrCreateSenderByType(logzioToken, logzioType, drainTimeout, fsPercentThreshold, bufferDir, logzioUrl, socketTimeout, connectTimeout, debug, reporter, tasksExecutor, gcPersistedQueueFilesIntervalSeconds, false);
    }

    public void start() {
        tasksExecutor.scheduleWithFixedDelay(this::drainQueueAndSend, 0, drainTimeout, TimeUnit.SECONDS);
    }

    public void stop() {
        // Creating a scheduled executor, outside of logback to try and drain the queue one last time
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        debug("Got stop request, Submitting a final drain queue task to drain before shutdown. Will timeout in " + FINAL_DRAIN_TIMEOUT_SEC + " seconds.");

        try {
            executorService.submit(this::drainQueue).get(FINAL_DRAIN_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            debug("Waited " + FINAL_DRAIN_TIMEOUT_SEC + " seconds, but could not finish draining. quitting.", e);
        } finally {
            executorService.shutdownNow();
        }
    }


    public void drainQueueAndSend() {
        try {
            if (drainRunning.get()) {
                debug("Drain is running so we won't run another one in parallel");
                return;
            } else {
                drainRunning.set(true);
            }

            drainQueue();

        } catch (Exception e) {
            // We cant throw anything out, or the task will stop, so just swallow all
            reporter.error("Uncaught error from Logz.io sender", e);
        } finally {
            drainRunning.set(false);
        }
    }

    public void send(JsonObject jsonMessage) {
        // Return the json, while separating lines with \n
        logsBuffer.enqueue((jsonMessage+ "\n").getBytes());
    }

    private List<FormattedLogMessage> dequeueUpToMaxBatchSize() {
        List<FormattedLogMessage> logsList = new ArrayList<>();
        int totalSize = 0;
        while (!logsBuffer.isEmpty()) {
            byte[] message  = logsBuffer.dequeue();
            if (message != null && message.length > 0) {
                logsList.add(new FormattedLogMessage(message));
                totalSize += message.length;
                if (totalSize >= MAX_SIZE_IN_BYTES) {
                    break;
                }
            }
        }
        return logsList;
    }

    private void drainQueue() {
        debug("Attempting to drain queue");
        if (!logsBuffer.isEmpty()) {
            while (!logsBuffer.isEmpty()) {
                List<FormattedLogMessage> logsList = dequeueUpToMaxBatchSize();
                try {
                    httpsSyncSender.sendToLogzio(logsList);

                } catch (LogzioServerErrorException e) {
                    debug("Could not send log to logz.io: ", e);
                    debug("Will retry in the next interval");

                    // And lets return everything to the queue
                    logsList.forEach((logMessage) -> logsBuffer.enqueue(logMessage.getMessage()));

                    // Lets wait for a new interval, something is wrong in the server side
                    break;
                }
                if (Thread.interrupted()) {
                    debug("Stopping drainQueue to thread being interrupted");
                    break;
                }
            }
        }
    }

    private void debug(String message) {
        if (debug) {
            reporter.info("DEBUG: " + message);
        }
    }

    private void debug(String message, Throwable e) {
        if (debug) {
            reporter.info("DEBUG: " + message, e);
        }
    }


    public static class Builder {
        private int inMemoryBufferThreshold = 1024 * 1024 * 100; //100MB memory limit
        private String logzioToken;
        private String logzioType;
        private int drainTimeout = 5; //sec
        private int fsPercentThreshold = 98;
        private File bufferDir;
        private String logzioUrl = "https://listener.logz.io:8071";
        private int socketTimeout = 10 * 1000;
        private int connectTimeout = 10 * 1000;
        private boolean debug = false;
        private SenderStatusReporter reporter;
        private ScheduledExecutorService tasksExecutor;
        private boolean compressRequests = false;
        private int gcPersistedQueueFilesIntervalSeconds = 30;
        private LogzioLogsBufferInterface buffer = null;

        public Builder setInMemoryBufferThreshold(int inMemoryBufferThreshold) {
            this.inMemoryBufferThreshold = inMemoryBufferThreshold;
            return this;
        }

        public Builder setLogzioToken(String logzioToken) {
            this.logzioToken = logzioToken;
            return this;
        }

        public Builder setLogzioType(String logzioType) {
            this.logzioType = logzioType;
            return this;
        }

        public Builder setDrainTimeout(int drainTimeout) {
            this.drainTimeout = drainTimeout;
            return this;
        }

        public Builder setFsPercentThreshold(int fsPercentThreshold) {
            this.fsPercentThreshold = fsPercentThreshold;
            return this;
        }

        public Builder setBufferDir(File bufferDir) {
            this.bufferDir = bufferDir;
            return this;
        }

        public Builder setLogzioUrl(String logzioUrl) {
            this.logzioUrl = logzioUrl;
            return this;
        }

        public Builder setSocketTimeout(int socketTimeout) {
            this.socketTimeout = socketTimeout;
            return this;
        }

        public Builder setConnectTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder setDebug(boolean debug) {
            this.debug = debug;
            return this;
        }

        public Builder setTasksExecutor(ScheduledExecutorService tasksExecutor) {
            this.tasksExecutor = tasksExecutor;
            return this;
        }

        public Builder setCompressRequests(boolean compressRequests) {
            this.compressRequests = compressRequests;
            return this;
        }

        public Builder setReporter(SenderStatusReporter reporter) {
            this.reporter = reporter;
            return this;
        }


        public Builder setLogsBuffer(LogzioLogsBufferInterface buffer) {
            this.buffer = buffer;
            return this;
        }

        public Builder setGcPersistedQueueFilesIntervalSeconds(int gcPersistedQueueFilesIntervalSeconds) {
            this.gcPersistedQueueFilesIntervalSeconds = gcPersistedQueueFilesIntervalSeconds;
            return this;
        }

        public LogzioSender build() throws LogzioParameterErrorException {
            return  getLogzioSender(
                    logzioToken,
                    logzioType,
                    drainTimeout,
                    fsPercentThreshold,
                    bufferDir,
                    logzioUrl,
                    socketTimeout,
                    connectTimeout,
                    debug,
                    reporter,
                    tasksExecutor,
                    gcPersistedQueueFilesIntervalSeconds,
                    compressRequests,
                    buffer == null ?
                            getDefaultQueue(bufferDir, fsPercentThreshold, reporter, gcPersistedQueueFilesIntervalSeconds)
                            : buffer,
                    inMemoryBufferThreshold);
        }
    }
    private static LogzioLogsBufferInterface getDefaultQueue(File bufferDir, int fsPercentThreshold,
                                                             SenderStatusReporter reporter,
                                                             int gcPersistedQueueFilesIntervalSeconds)
            throws LogzioParameterErrorException {

        return DiskQueue
                .builder()
                .setBufferDir(bufferDir)
                .setFsPercentThreshold(fsPercentThreshold)
                .setReporter(reporter)
                .setGcPersistedQueueFilesIntervalSeconds(gcPersistedQueueFilesIntervalSeconds)
                .build();
    }

    public static Builder builder(){
        return new Builder();
    }

}
