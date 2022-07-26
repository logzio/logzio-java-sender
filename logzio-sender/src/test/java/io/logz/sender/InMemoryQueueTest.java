package io.logz.sender;

import com.google.gson.JsonObject;
import io.logz.sender.LogzioSender.Builder;
import io.logz.sender.exceptions.LogzioParameterErrorException;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.logz.sender.LogzioTestSenderUtil.createJsonMessage;

public class InMemoryQueueTest extends LogzioSenderTest {
    private final static long defaultCapacityInBytes = 100 * 1024 * 1024;
    private final static int MAX_LOG_LINE_SIZE_IN_BYTES = 32700;
    private final static String TRUNCATED_MESSAGE_SUFFIX = "...truncated";
    private final static String EXCEEDING_MESSAGE_FILE_PATH = "src/test/resources/exceeding_max_size_message.log";

    @Override
    protected Builder getLogzioSenderBuilder(String token, String type, Integer drainTimeout,
                                             Integer socketTimeout, Integer serverTimeout,
                                             ScheduledExecutorService tasks, boolean compressRequests)
            throws LogzioParameterErrorException {

        Builder logzioSenderBuilder = super.getLogzioSenderBuilder(token, type, drainTimeout,
                socketTimeout, serverTimeout, tasks, compressRequests);

        setCapacityInBytes(logzioSenderBuilder, defaultCapacityInBytes);
        return logzioSenderBuilder;
    }

    @Override
    protected void setZeroThresholdQueue(Builder logzioSenderBuilder) {
        setCapacityInBytes(logzioSenderBuilder, 0);
    }

    private void setCapacityInBytes(Builder logzioSenderBuilder, long capacityInBytes) {
        logzioSenderBuilder
                .withInMemoryQueue()
                .setCapacityInBytes(capacityInBytes)
                .endInMemoryQueue();
    }

    private void setLogsCountLimit(Builder logzioSenderBuilder, long logsCounterLimit) {
        logzioSenderBuilder
                .withInMemoryQueue()
                .setLogsCountLimit(logsCounterLimit)
                .endInMemoryQueue();
    }

    @Test
    public void checkCapacityReachedToSizeBelowCapacity() throws LogzioParameterErrorException {
        String token = "checkCrossCapacityInBytes";
        String type = random(8);
        String loggerName = "checkCrossCapacityInBytesName";
        int drainTimeout = 2;
        int successfulLogs = 3;

        String message = "Log before drop - " + random(5);
        JsonObject log = createJsonMessage(loggerName, message);

        int logSize = log.toString().getBytes(StandardCharsets.UTF_8).length;
        ScheduledExecutorService tasks = Executors.newScheduledThreadPool(3);

        Builder testSenderBuilder = getLogzioSenderBuilder(token, type, drainTimeout, 10 * 1000,
                10 * 1000, tasks, false);
        setCapacityInBytes(testSenderBuilder, logSize * successfulLogs);

        LogzioSender testSender = createLogzioSender(testSenderBuilder);

        sleepSeconds(drainTimeout - 1);
        for (int i = 0; i <= successfulLogs; i++) {
            testSender.send(log);
        }

        sleepSeconds(2 * drainTimeout);
        mockListener.assertNumberOfReceivedMsgs(successfulLogs);

        sleepSeconds(2 * drainTimeout);
        testSender.send(log);
        sleepSeconds(2 * drainTimeout);
        mockListener.assertNumberOfReceivedMsgs(successfulLogs + 1);
        tasks.shutdownNow();
    }

    @Test
    public void checkLogMessageCountLimitWithCapacityInBytes() throws LogzioParameterErrorException {
        String token = "checkLogMessageCountLimitOnly";
        String type = random(8);
        String loggerName = "checkLogMessageCountLimitOnly";
        int drainTimeout = 2;
        int successfulLogs = 3;

        String message = "Log before drop - " + random(5);
        JsonObject log = createJsonMessage(loggerName, message);

        ScheduledExecutorService tasks = Executors.newScheduledThreadPool(3);

        Builder testSenderBuilder = getLogzioSenderBuilder(token, type, drainTimeout, 10 * 1000,
                10 * 1000, tasks, false);
        setLogsCountLimit(testSenderBuilder, successfulLogs);

        LogzioSender testSender = createLogzioSender(testSenderBuilder);

        sleepSeconds(drainTimeout - 1);
        for (int i = 0; i <= successfulLogs; i++) {
            testSender.send(log);
        }

        sleepSeconds(2 * drainTimeout);
        mockListener.assertNumberOfReceivedMsgs(successfulLogs);

        sleepSeconds(2 * drainTimeout);
        testSender.send(log);
        sleepSeconds(2 * drainTimeout);
        mockListener.assertNumberOfReceivedMsgs(successfulLogs + 1);
        tasks.shutdownNow();
    }

    @Test
    public void checkExceedingMaxSizeJsonLogWithCut() throws LogzioParameterErrorException, IOException {
        String token = "checkExceedingMaxSizeJsonLogWithCut";
        String type = random(8);
        String loggerName = "checkExceedingMaxSizeJsonLogWithCutName";
        int drainTimeout = 2;

        String message = Files.readString(Path.of(EXCEEDING_MESSAGE_FILE_PATH));
        JsonObject log = createJsonMessage(loggerName, message);

        int logSize = log.toString().getBytes(StandardCharsets.UTF_8).length;
        ScheduledExecutorService tasks = Executors.newScheduledThreadPool(1);

        Builder testSenderBuilder = getLogzioSenderBuilder(token, type, drainTimeout, 10 * 1000,
                10 * 1000, tasks, false);
        testSenderBuilder.setExceedMaxSizeAction("cut");
        setCapacityInBytes(testSenderBuilder, logSize);

        LogzioSender testSender = createLogzioSender(testSenderBuilder);

        testSender.send(log);
        sleepSeconds(2 * drainTimeout);
        mockListener.assertLogReceivedByMessage(message.substring(0, MAX_LOG_LINE_SIZE_IN_BYTES - TRUNCATED_MESSAGE_SUFFIX.length()) + TRUNCATED_MESSAGE_SUFFIX);
        tasks.shutdownNow();
    }

    @Test
    public void checkExceedingMaxSizeBytesLogWithCut() throws LogzioParameterErrorException, IOException {
        String token = "checkExceedingMaxSizeBytesLogWithCut";
        String type = random(8);
        String loggerName = "checkExceedingMaxSizeBytesLogWithCutName";
        int drainTimeout = 2;

        String message = Files.readString(Path.of(EXCEEDING_MESSAGE_FILE_PATH));
        JsonObject log = createJsonMessage(loggerName, message);

        int logSize = log.toString().getBytes(StandardCharsets.UTF_8).length;
        ScheduledExecutorService tasks = Executors.newScheduledThreadPool(1);

        Builder testSenderBuilder = getLogzioSenderBuilder(token, type, drainTimeout, 10 * 1000,
                10 * 1000, tasks, false);
        testSenderBuilder.setExceedMaxSizeAction("cut");
        setCapacityInBytes(testSenderBuilder, logSize);

        LogzioSender testSender = createLogzioSender(testSenderBuilder);

        testSender.send(log.toString().getBytes(StandardCharsets.UTF_8));
        sleepSeconds(2 * drainTimeout);
        mockListener.assertLogReceivedByMessage(message.substring(0, MAX_LOG_LINE_SIZE_IN_BYTES - TRUNCATED_MESSAGE_SUFFIX.length()) + TRUNCATED_MESSAGE_SUFFIX);
        tasks.shutdownNow();
    }

    @Test
    public void checkExceedingMaxSizeJsonLogWithDrop() throws LogzioParameterErrorException, IOException {
        String token = "checkExceedingMaxSizeJsonLogWithDrop";
        String type = random(8);
        String loggerName = "checkExceedingMaxSizeJsonLogWithDropName";
        int drainTimeout = 2;

        String message = Files.readString(Path.of(EXCEEDING_MESSAGE_FILE_PATH));
        JsonObject log = createJsonMessage(loggerName, message);

        int logSize = log.toString().getBytes(StandardCharsets.UTF_8).length;
        ScheduledExecutorService tasks = Executors.newScheduledThreadPool(1);

        Builder testSenderBuilder = getLogzioSenderBuilder(token, type, drainTimeout, 10 * 1000,
                10 * 1000, tasks, false);
        testSenderBuilder.setExceedMaxSizeAction("drop");
        setCapacityInBytes(testSenderBuilder, logSize);

        LogzioSender testSender = createLogzioSender(testSenderBuilder);

        testSender.send(log);
        sleepSeconds(2 * drainTimeout);
        mockListener.assertNumberOfReceivedMsgs(0);
        tasks.shutdownNow();
    }

    @Test
    public void checkExceedingMaxSizeBytesLogWithDrop() throws LogzioParameterErrorException, IOException {
        String token = "checkExceedingMaxSizeBytesLogWithDrop";
        String type = random(8);
        String loggerName = "checkExceedingMaxSizeBytesLogWithDropName";
        int drainTimeout = 2;

        String message = Files.readString(Path.of(EXCEEDING_MESSAGE_FILE_PATH));
        JsonObject log = createJsonMessage(loggerName, message);

        int logSize = log.toString().getBytes(StandardCharsets.UTF_8).length;
        ScheduledExecutorService tasks = Executors.newScheduledThreadPool(1);

        Builder testSenderBuilder = getLogzioSenderBuilder(token, type, drainTimeout, 10 * 1000,
                10 * 1000, tasks, false);
        testSenderBuilder.setExceedMaxSizeAction("drop");
        setCapacityInBytes(testSenderBuilder, logSize);

        LogzioSender testSender = createLogzioSender(testSenderBuilder);

        testSender.send(log.toString().getBytes(StandardCharsets.UTF_8));
        sleepSeconds(2 * drainTimeout);
        mockListener.assertNumberOfReceivedMsgs(0);
        tasks.shutdownNow();
    }
}

