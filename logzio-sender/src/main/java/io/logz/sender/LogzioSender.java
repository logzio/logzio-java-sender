package io.logz.sender;

import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.logz.sender.exceptions.LogzioParameterErrorException;
import io.logz.sender.exceptions.LogzioServerErrorException;
import io.logz.sender.model.RTQuery;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.Collectors;

public class LogzioSender  {
    private static final int MAX_SIZE_IN_BYTES = 3 * 1024 * 1024;  // 3 MB

    private static final Map<AbstractMap.SimpleImmutableEntry<String, String>, LogzioSender> logzioSenderInstances = new HashMap<>();
    private static final int FINAL_DRAIN_TIMEOUT_SEC = 20;

    private final RealTimeFiltersQueue logsQueue;
    private final int drainTimeout;
    private final int pollInterval;
    private final boolean debug;
    private final SenderStatusReporter reporter;
    private ScheduledExecutorService tasksExecutor;
    private final AtomicBoolean drainRunning = new AtomicBoolean(false);
    private final HttpsSyncSender httpsSyncSender;
    private List<RTFilter> realTimeQueryRTFilters;
    private OkHttpClient httpClient;
    private Gson gson = new Gson();
    private String rtQueriesUrlProvider;
    private String rtQueriesAPIToken;
    private String hostname;

    private LogzioSender(HttpsRequestConfiguration httpsRequestConfiguration, int drainTimeout, int pollInterval, boolean debug,
                         SenderStatusReporter reporter, ScheduledExecutorService tasksExecutor,
                         RealTimeFiltersQueue logsQueue, String rtQueriesUrlProvider, String rtQueriesAPIToken, String hostname) throws LogzioParameterErrorException {

        if (logsQueue == null || reporter == null || httpsRequestConfiguration == null) {
            throw new LogzioParameterErrorException("logsQueue=" + logsQueue + " reporter=" + reporter
                    + " httpsRequestConfiguration=" + httpsRequestConfiguration ,
                    "For some reason could not initialize URL. Cant recover..");
        }

        this.logsQueue = logsQueue;
        this.drainTimeout = drainTimeout;
        this.pollInterval = pollInterval;
        this.debug = debug;
        this.reporter = reporter;
        httpsSyncSender = new HttpsSyncSender(httpsRequestConfiguration, reporter);
        this.tasksExecutor = tasksExecutor;
        this.httpClient = new OkHttpClient.Builder().build();

        this.rtQueriesUrlProvider = rtQueriesUrlProvider;
        this.rtQueriesAPIToken = rtQueriesAPIToken;
        this.hostname = hostname;

        debug("Created new LogzioSender class");
    }

    private static LogzioSender getLogzioSender(HttpsRequestConfiguration httpsRequestConfiguration, int drainTimeout, int pollInterval, boolean debug, SenderStatusReporter reporter,
                                                ScheduledExecutorService tasksExecutor, RealTimeFiltersQueue logsQueue, String rtQueriesUrlProvider,
                                                String rtQueriesAPIToken, String hostname)
            throws LogzioParameterErrorException {
        String tokenHash = Hashing.sha256()
                .hashString(httpsRequestConfiguration.getLogzioToken(), StandardCharsets.UTF_8)
                .toString()
                .substring(0,7);
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

            LogzioSender logzioSender = new LogzioSender(httpsRequestConfiguration, drainTimeout, pollInterval, debug, reporter,
                    tasksExecutor, logsQueue, rtQueriesUrlProvider, rtQueriesAPIToken, hostname);
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
        tasksExecutor.scheduleWithFixedDelay(this::pollRealTimeQueries, 0, pollInterval, TimeUnit.SECONDS);
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

    public void pollRealTimeQueries() {
        List<RTFilter> rtFilters = searchRTFilters();
        logsQueue.setRTQueryFilters(rtFilters);
    }

    private List<RTFilter> searchRTFilters() {
        List<RTQuery> rtQueries = searchRTQueries();
        return rtQueries.stream().map(RTQuery::getQuery).map(JsonPathRTFilter::new).collect(Collectors.toList());
    }

    private List<RTQuery> searchRTQueries() {

        String requestJson = "{}";

        if (hostname != null) {
            requestJson = "{\"hostname\" : \"" + hostname + "\"}";
        }

        RequestBody body = RequestBody.create(
        MediaType.parse("application/json"), requestJson);

        Request request = new Request.Builder()
                .url("http://" + rtQueriesUrlProvider )
                .addHeader("X-API-TOKEN", rtQueriesAPIToken)
                .post(body)
                .build();

        try {
            ResponseBody responseBody = httpClient.newCall(request).execute().body();
            RTQuery[] rtQueryArray = gson.fromJson(responseBody.string(), RTQuery[].class);
            return Arrays.asList(rtQueryArray.clone());
        } catch (Exception e) {
            throw new RuntimeException("failed polling real time queries");
        }
    }

    public void send(JsonObject jsonMessage) {
        // Return the json, while separating lines with \n
        logsQueue.enqueue(jsonMessage.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Send byte array to Logz.io
     * This method is not the recommended method to use
     * since it is up to the user to supply with a valid UTF8 json byte array
     * representation. In any case the byte[] is not valid, the logs will not be sent.
     * @param jsonStringAsUTF8ByteArray UTF8 byte array representation of a valid json object.
     */
    public void send(byte[] jsonStringAsUTF8ByteArray) {
        // Return the byte[], while separating lines with \n
        logsQueue.enqueue(jsonStringAsUTF8ByteArray);
    }

    private List<FormattedLogMessage> dequeueUpToMaxBatchSize() {
        List<FormattedLogMessage> logsList = new ArrayList<>();
        int totalSize = 0;
        while (!logsQueue.isEmpty()) {
            byte[] message  = logsQueue.dequeue();
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
                    logsList.forEach((logMessage) -> logsQueue.enqueue(logMessage.getMessage()));

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
        private int pollIntervalSec = 5;
        private SenderStatusReporter reporter;
        private ScheduledExecutorService tasksExecutor;
        private InMemoryQueue.Builder inMemoryQueueBuilder;
        private DiskQueue.Builder diskQueueBuilder;
        private HttpsRequestConfiguration httpsRequestConfiguration;
        private String[] defaultFilters = new String[0];
        private String rtQueriesUrlProvider = "127.0.0.1:9990/real-time-queries/search";
        private String rtQueriesAPIToken;
        private String hostname = null;

        public Builder setHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder setRTQueriesUrlProvider(String url) {
            this.rtQueriesUrlProvider = url;
            return this;
        }

        public Builder setRTQueriesAPIToken(String token) {
            this.rtQueriesAPIToken = token;
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

        public Builder setDefaultFilters(String[] defaultFilters) {
            this.defaultFilters = defaultFilters;
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

        public LogzioSender build() throws LogzioParameterErrorException {
            return  getLogzioSender(
                    httpsRequestConfiguration,
                    drainTimeoutSec,
                    pollIntervalSec,
                    debug,
                    reporter,
                    tasksExecutor,
                    getLogsQueue(),
                    rtQueriesUrlProvider,
                    rtQueriesAPIToken,
                    hostname
            );
        }

        private RealTimeFiltersQueue getLogsQueue() throws LogzioParameterErrorException {
            RealTimeFiltersQueue.Builder rtfQueueBuilder = new RealTimeFiltersQueue.Builder();
            if (diskQueueBuilder != null) {
                diskQueueBuilder.setDiskSpaceTasks(tasksExecutor);
                diskQueueBuilder.setReporter(reporter);
                rtfQueueBuilder.setFilteredQueue(diskQueueBuilder.build());
            } else {
                inMemoryQueueBuilder.setReporter(reporter);
                rtfQueueBuilder.setFilteredQueue(inMemoryQueueBuilder.build());
            }
            rtfQueueBuilder.setReporter(reporter);
            List<RTFilter> defaultRTFilters = new ArrayList<>();
            if (defaultFilters != null) {
                for (String strFilter : this.defaultFilters) {
                    defaultRTFilters.add(new JsonPathRTFilter(strFilter));
                }
            }
            rtfQueueBuilder.setDefaultFilters(defaultRTFilters);
            return rtfQueueBuilder.build();
        }
    }

    public static Builder builder(){
        return new Builder();
    }

}
