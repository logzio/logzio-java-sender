package io.logz.sender;

import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import io.logz.sender.exceptions.LogzioParameterErrorException;
import io.logz.sender.exceptions.LogzioServerErrorException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class LogzioSender {
    private static final int MAX_SIZE_IN_BYTES = 3 * 1024 * 1024;  // 3 MB
    private static final int MAX_LOG_SIZE_IN_BYTES = 500000;
    private static final int MAX_LOG_LINE_SIZE_IN_BYTES = 32700;
    private static final String CUT_EXCEEDING_LOG = "cut";
    private static final String DROP_EXCEEDING_LOG = "drop";
    private static final String TRUNCATED_MESSAGE_SUFFIX = "...truncated";
    private static final Map<AbstractMap.SimpleImmutableEntry<String, String>, LogzioSender> logzioSenderInstances = new HashMap<>();
    private static final int FINAL_DRAIN_TIMEOUT_SEC = 20;

    private final LogsQueue logsQueue;
    private final int drainTimeout;
    private final String exceedMaxSizeAction;
    private final boolean debug;
    private final SenderStatusReporter reporter;
    private ScheduledExecutorService tasksExecutor;
    private final AtomicBoolean drainRunning = new AtomicBoolean(false);
    private final HttpsSyncSender httpsSyncSender;

    private LogzioSender(HttpsRequestConfiguration httpsRequestConfiguration, int drainTimeout, boolean debug,
                         SenderStatusReporter reporter, ScheduledExecutorService tasksExecutor,
                         LogsQueue logsQueue, String exceedMaxSizeAction) throws LogzioParameterErrorException {

        if (logsQueue == null || reporter == null || httpsRequestConfiguration == null) {
            throw new LogzioParameterErrorException("logsQueue=" + logsQueue + " reporter=" + reporter
                    + " httpsRequestConfiguration=" + httpsRequestConfiguration,
                    "For some reason could not initialize URL. Cant recover..");
        }

        this.exceedMaxSizeAction = validateAndGetExceedMaxSizeAction(exceedMaxSizeAction);
        this.logsQueue = logsQueue;
        this.drainTimeout = drainTimeout;
        this.debug = debug;
        this.reporter = reporter;
        httpsSyncSender = new HttpsSyncSender(httpsRequestConfiguration, reporter);
        this.tasksExecutor = tasksExecutor;
        debug("Created new LogzioSender class");
    }

    private String validateAndGetExceedMaxSizeAction(String exceedMaxSizeAction) throws LogzioParameterErrorException {
        if (exceedMaxSizeAction != null && Arrays.asList("cut", "drop").contains(exceedMaxSizeAction.toLowerCase())) {
            return exceedMaxSizeAction.toLowerCase();
        }

        throw new LogzioParameterErrorException("exceedMaxSizeAction=" + exceedMaxSizeAction, "invalid parameter value");
    }

    private static LogzioSender getLogzioSender(HttpsRequestConfiguration httpsRequestConfiguration, int drainTimeout, boolean debug, SenderStatusReporter reporter,
                                                ScheduledExecutorService tasksExecutor, LogsQueue logsQueue, String exceedMaxSizeAction)
            throws LogzioParameterErrorException {
        String tokenHash = Hashing.sha256()
                .hashString(httpsRequestConfiguration.getLogzioToken(), StandardCharsets.UTF_8)
                .toString()
                .substring(0, 7);
        AbstractMap.SimpleImmutableEntry<String, String> tokenAndTypePair = new AbstractMap.SimpleImmutableEntry<>
                (tokenHash, httpsRequestConfiguration.getLogzioType());
        // We want one queue per logzio token and data type.
        // so that's why I create separate queues per token and data type.
        // BUT - users not always understand the notion of types at first, and can define multiple data sender on the same type - and this is what I want to protect by this factory.
        LogzioSender logzioSenderInstance = logzioSenderInstances.get(tokenAndTypePair);
        if (logzioSenderInstance == null) {
            if (logsQueue == null) {
                throw new LogzioParameterErrorException("logsQueue", "null");
            }

            LogzioSender logzioSender = new LogzioSender(httpsRequestConfiguration, drainTimeout, debug, reporter,
                    tasksExecutor, logsQueue, exceedMaxSizeAction);
            logzioSenderInstances.put(tokenAndTypePair, logzioSender);
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

    public void clearQueue() throws IOException {
        this.logsQueue.clear();
    }

    public void send(JsonObject jsonMessage) {

        // check for oversized message
        int jsonByteLength = jsonMessage.toString().getBytes(StandardCharsets.UTF_8).length;
        String jsonMessageField = jsonMessage.get("message").getAsString();
        if (jsonByteLength > MAX_LOG_SIZE_IN_BYTES || jsonMessageField.length() >= MAX_LOG_LINE_SIZE_IN_BYTES) {

            // calculate the minimum between max log line size, and the message field after truncating the exceeding bytes
            int truncatedMessageSize = Math.min(MAX_LOG_LINE_SIZE_IN_BYTES - TRUNCATED_MESSAGE_SUFFIX.length(),
                    (jsonMessageField.getBytes(StandardCharsets.UTF_8).length - (jsonByteLength - MAX_LOG_SIZE_IN_BYTES)) - TRUNCATED_MESSAGE_SUFFIX.length());

            if (truncatedMessageSize <= 0 || exceedMaxSizeAction.equals(DROP_EXCEEDING_LOG)) {
                debug(truncatedMessageSize <= 0 ? "Message field is empty after truncating, dropping log" : "Dropping oversized log");
                return;
            }

            // truncate message field
            String truncatedMessage = jsonMessageField.substring(0, truncatedMessageSize) + TRUNCATED_MESSAGE_SUFFIX;
            jsonMessage.addProperty("message", truncatedMessage);
            debug("Truncated oversized log");
        }

        logsQueue.enqueue(jsonMessage.toString().getBytes(StandardCharsets.UTF_8));
    }


    /**
     * Send byte array to Logz.io
     * This method is not the recommended method to use
     * since it is up to the user to supply with a valid UTF8 json byte array
     * representation, and converting the byte array to JsonObject is necessary for log size validation.
     * In any case the byte[] is not valid, the logs will not be sent.
     *
     * @param jsonStringAsUTF8ByteArray UTF8 byte array representation of a valid json object.
     */
    public void send(byte[] jsonStringAsUTF8ByteArray) {
        Gson gson = new Gson();
        boolean dropLog = false;
        try {
            if (jsonStringAsUTF8ByteArray.length > MAX_LOG_SIZE_IN_BYTES) {
                if (exceedMaxSizeAction.equals(CUT_EXCEEDING_LOG)) {
                    String jsonString = new String(jsonStringAsUTF8ByteArray, StandardCharsets.UTF_8);
                    JsonObject json = gson.fromJson(jsonString, JsonElement.class).getAsJsonObject();
                    String messageString = json.get("message").getAsString();
                    int truncatedMessageSize = Math.min(MAX_LOG_LINE_SIZE_IN_BYTES - TRUNCATED_MESSAGE_SUFFIX.length(),
                            (messageString.getBytes(StandardCharsets.UTF_8).length -
                                    (jsonString.getBytes(StandardCharsets.UTF_8).length - MAX_LOG_SIZE_IN_BYTES)) - TRUNCATED_MESSAGE_SUFFIX.length());
                    if (truncatedMessageSize <= 0) {
                        dropLog = true;
                    } else {
                        String truncatedMessage = messageString.substring(0, truncatedMessageSize) + TRUNCATED_MESSAGE_SUFFIX;
                        json.addProperty("message", truncatedMessage);
                        jsonStringAsUTF8ByteArray = json.toString().getBytes(StandardCharsets.UTF_8);
                        debug("Truncated oversized log");
                    }
                }
            }
            // Invalid json format, or truncating the message field was no
        } catch (JsonSyntaxException | IndexOutOfBoundsException e) {
            dropLog = true;
        }

        if (dropLog || exceedMaxSizeAction.equals(DROP_EXCEEDING_LOG)) {
            debug(dropLog ? "Message field is empty after truncating, dropping log" : "Dropping oversized log");
            return;
        }
        // Return the byte[], while separating lines with \n
        logsQueue.enqueue(jsonStringAsUTF8ByteArray);

    }

    private List<FormattedLogMessage> dequeueUpToMaxBatchSize() {
        List<FormattedLogMessage> logsList = new ArrayList<>();
        int totalSize = 0;
        while (!logsQueue.isEmpty()) {
            byte[] message = logsQueue.dequeue();
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
        if (!logsQueue.isEmpty()) {
            while (!logsQueue.isEmpty()) {
                List<FormattedLogMessage> logsList = dequeueUpToMaxBatchSize();
                try {
                    httpsSyncSender.sendToLogzio(logsList);
                } catch (LogzioServerErrorException e) {
                    debug("Could not send log to logz.io: ", e);
                    debug("Will retry in the next interval");

                    // And lets return everything to the queue
                    logsList.forEach((logMessage) -> {
                        logsQueue.enqueue(logMessage.getMessage());
                    });

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
        private int drainTimeoutSec = 5;
        private SenderStatusReporter reporter;
        private ScheduledExecutorService tasksExecutor;
        private InMemoryQueue.Builder inMemoryQueueBuilder;
        private DiskQueue.Builder diskQueueBuilder;
        private HttpsRequestConfiguration httpsRequestConfiguration;
        private String exceedMaxSizeAction = "cut";

        public Builder setExceedMaxSizeAction(String exceedMaxSizeAction) {
            this.exceedMaxSizeAction = exceedMaxSizeAction;
            return this;
        }

        public Builder setDrainTimeoutSec(int drainTimeoutSec) {
            this.drainTimeoutSec = drainTimeoutSec;
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

        public Builder setHttpsRequestConfiguration(HttpsRequestConfiguration httpsRequestConfiguration) {
            this.httpsRequestConfiguration = httpsRequestConfiguration;
            return this;
        }

        public InMemoryQueue.Builder withInMemoryQueue() {
            if (this.inMemoryQueueBuilder == null) {
                this.inMemoryQueueBuilder = InMemoryQueue.builder(this);
            }
            return this.inMemoryQueueBuilder;
        }

        public DiskQueue.Builder withDiskQueue() {
            if (this.diskQueueBuilder == null) {
                this.diskQueueBuilder = DiskQueue.builder(this, tasksExecutor);
            }
            return this.diskQueueBuilder;
        }

        void setDiskQueueBuilder(DiskQueue.Builder diskQueueBuilder) {
            this.diskQueueBuilder = diskQueueBuilder;
        }

        void setInMemoryQueueBuilder(InMemoryQueue.Builder inMemoryQueueBuilder) {
            this.inMemoryQueueBuilder = inMemoryQueueBuilder;
        }

        public LogzioSender build() throws LogzioParameterErrorException, IOException {
            return getLogzioSender(
                    httpsRequestConfiguration,
                    drainTimeoutSec,
                    debug,
                    reporter,
                    tasksExecutor,
                    getLogsQueue(),
                    exceedMaxSizeAction
            );
        }

        private LogsQueue getLogsQueue() throws LogzioParameterErrorException, IOException {
            if (diskQueueBuilder != null) {
                diskQueueBuilder.setDiskSpaceTasks(tasksExecutor);
                diskQueueBuilder.setReporter(reporter);
                return diskQueueBuilder.build();
            }

            inMemoryQueueBuilder.setReporter(reporter);
            return inMemoryQueueBuilder.build();
        }

    }

    public static Builder builder() {
        return new Builder();
    }

}
