package io.logz.sender;

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


    private LogzioSender(HttpsRequestConfiguration httpsRequestConfiguration, int drainTimeout, boolean debug,
                         SenderStatusReporter reporter, ScheduledExecutorService tasksExecutor,
                         LogzioLogsBufferInterface logsBuffer) throws LogzioParameterErrorException {

        if (logsBuffer == null || reporter == null || httpsRequestConfiguration == null) {
            throw new LogzioParameterErrorException("logsBuffer=" + logsBuffer + " reporter=" + reporter
                    + " httpsRequestConfiguration=" + httpsRequestConfiguration ,
                    "For some reason could not initialize URL. Cant recover..");
        }

        this.logsBuffer = logsBuffer;
        this.drainTimeout = drainTimeout;
        this.debug = debug;
        this.reporter = reporter;
        httpsSyncSender = new HttpsSyncSender(httpsRequestConfiguration, reporter);
        this.tasksExecutor = tasksExecutor == null ? Executors.newSingleThreadScheduledExecutor() : tasksExecutor;
        debug("Created new LogzioSender class");
    }

    @Deprecated
    public static synchronized LogzioSender getOrCreateSenderByType(String logzioToken, String logzioType,
                                                                    int drainTimeout, int fsPercentThreshold,
                                                                    File bufferDir, String logzioUrl, int socketTimeout,
                                                                    int connectTimeout, boolean debug,
                                                                    SenderStatusReporter reporter,
                                                                    ScheduledExecutorService tasksExecutor,
                                                                    int gcPersistedQueueFilesIntervalSeconds,
                                                                    boolean compressRequests)
            throws LogzioParameterErrorException {

        LogzioLogsBufferInterface logsBuffer = null;
        if (bufferDir != null) {
            logsBuffer = DiskQueue
                    .builder()
                    .setGcPersistedQueueFilesIntervalSeconds(gcPersistedQueueFilesIntervalSeconds)
                    .setReporter(reporter)
                    .setFsPercentThreshold(fsPercentThreshold)
                    .setBufferDir(bufferDir)
                    .build();
        }
        HttpsRequestConfiguration httpsRequestConfiguration = HttpsRequestConfiguration
                .builder()
                .setCompressRequests(compressRequests)
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .setLogzioListenerUrl(logzioUrl)
                .setLogzioType(logzioType)
                .setLogzioToken(logzioToken)
                .build();
        return getLogzioSender(httpsRequestConfiguration, drainTimeout, debug, reporter, tasksExecutor, logsBuffer);


    }

    @Deprecated
    public static synchronized LogzioSender getOrCreateSenderByType(String logzioToken, String logzioType, int drainTimeout, int fsPercentThreshold, File bufferDir,
                                                                    String logzioUrl, int socketTimeout, int connectTimeout, boolean debug,
                                                                    SenderStatusReporter reporter, ScheduledExecutorService tasksExecutor,
                                                                    int gcPersistedQueueFilesIntervalSeconds) throws LogzioParameterErrorException {
        return getOrCreateSenderByType(logzioToken, logzioType, drainTimeout, fsPercentThreshold, bufferDir, logzioUrl, socketTimeout, connectTimeout, debug, reporter, tasksExecutor, gcPersistedQueueFilesIntervalSeconds, false);
    }

    private static LogzioSender getLogzioSender(HttpsRequestConfiguration httpsRequestConfiguration, int drainTimeout, boolean debug, SenderStatusReporter reporter,
                                                ScheduledExecutorService tasksExecutor, LogzioLogsBufferInterface logsBuffer)
            throws LogzioParameterErrorException {
        // We want one buffer per logzio data type.
        // so that's why I create separate buffers per type.
        // BUT - users not always understand the notion of types at first, and can define multiple data sender on the same type - and this is what I want to protect by this factory.
        LogzioSender logzioSenderInstance = logzioSenderInstances.get(httpsRequestConfiguration.getLogzioType());
        if (logzioSenderInstance == null) {
            if (logsBuffer == null) {
                throw new LogzioParameterErrorException("logsBuffer", "null");
            }

            LogzioSender logzioSender = new LogzioSender(httpsRequestConfiguration, drainTimeout, debug, reporter,
                    tasksExecutor, logsBuffer);
            logzioSenderInstances.put(httpsRequestConfiguration.getLogzioType(), logzioSender);
            return logzioSender;
        } else {
            reporter.info("Already found appender configured for type " + httpsRequestConfiguration.getLogzioType()
                    + ", re-using the same one.");

            // Sometimes (For example under Spring) the framework closes logback entirely (thos closing the executor)
            // So we need to take a new one instead, as we can grantee that nothing is running now because it is terminated.
            if (logzioSenderInstance.tasksExecutor.isTerminated()) {
                reporter.info("The old task executor is terminated! replacing it with a new one");
                logzioSenderInstance.tasksExecutor = tasksExecutor;
            }
            return logzioSenderInstance;
        }
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
        private boolean debug = false;
        private int drainTimeout = 5; //sec
        private SenderStatusReporter reporter;
        private ScheduledExecutorService tasksExecutor;
        private LogzioLogsBufferInterface logsBuffer;
        private HttpsRequestConfiguration httpsRequestConfiguration;

        public Builder setDrainTimeout(int drainTimeout) {
            this.drainTimeout = drainTimeout;
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


        public Builder setReporter(SenderStatusReporter reporter) {
            this.reporter = reporter;
            return this;
        }


        public Builder setLogsBuffer(LogzioLogsBufferInterface logsBuffer) {
            this.logsBuffer = logsBuffer;
            return this;
        }

        public Builder setHttpsRequestConfiguration(HttpsRequestConfiguration httpsRequestConfiguration) {
            this.httpsRequestConfiguration = httpsRequestConfiguration;
            return this;
        }

        public LogzioSender build() throws LogzioParameterErrorException {
            return  getLogzioSender(
                    httpsRequestConfiguration,
                    drainTimeout,
                    debug,
                    reporter,
                    tasksExecutor,
                    logsBuffer
            );
        }
    }

    public static Builder builder(){
        return new Builder();
    }

}
