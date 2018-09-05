package io.logz.sender;

import io.logz.sender.exceptions.LogzioParameterErrorException;
import io.logz.test.MockLogzioBulkListener;
import io.logz.test.TestEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.logz.sender.LogzioTestSenderUtil.LOGLEVEL;
import static io.logz.sender.LogzioTestSenderUtil.createJsonMessage;

public abstract class LogzioSenderTest {
    private MockLogzioBulkListener mockListener;
    private final static Logger logger = LoggerFactory.getLogger(LogzioSenderTest.class);
    private LogzioSender testSender;
    private static final int INITIAL_WAIT_BEFORE_RETRY_MS = 2000;
    private static final int MAX_RETRIES_ATTEMPTS = 3;
    private ScheduledExecutorService tasks;

    @Before
    public void preTest() throws Exception {
        mockListener = new MockLogzioBulkListener();
        mockListener.start();
        tasks = Executors.newScheduledThreadPool(3);
    }

    @After
    public void postTest() {
        if (mockListener != null)
             mockListener.stop();
        tasks.shutdownNow();
    }

    private void sleepSeconds(int seconds) {
        logger.info("Sleeping {} [sec]...", seconds);
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract LogzioSender createLogzioSender(String token, String type, Integer drainTimeout,
                                                       Integer socketTimeout, Integer serverTimeout,
                                                       ScheduledExecutorService tasks,
                                                       boolean compressRequests) throws LogzioParameterErrorException;

    protected abstract void setZeroThresholdBuffer() throws LogzioParameterErrorException;

    int getMockListenerPort() {
        return mockListener.getPort();
    }

    String getMockListenerHost() {
        return mockListener.getHost();
    }

    String random(int numberOfChars) {
        return UUID.randomUUID().toString().substring(0, numberOfChars-1);
    }

    @Test
    public void simpleAppending() throws Exception {
        String token = "aBcDeFgHiJkLmNoPqRsT";
        String type = random(8);
        String loggerName = "simpleAppending";
        int drainTimeout = 2;

        String message1 = "Testing.." + random(5);
        String message2 = "Warning test.." + random(5);

        LogzioSender testSender = createLogzioSender(token, type, drainTimeout,
                10 * 1000, 10 * 1000, tasks, false);

        testSender.send( createJsonMessage(loggerName, message1));
        testSender.send( createJsonMessage(loggerName, message2));
        sleepSeconds(drainTimeout  *3);

        mockListener.assertNumberOfReceivedMsgs(2);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);
        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
    }

    @Test
    public void simpleGzipAppending() throws Exception {
        String token = "gzipToken";
        String type = random(8);
        String loggerName = "simpleGzipAppending";
        int drainTimeout = 2;

        String message1 = "Testing.." + random(5);
        String message2 = "Warning test.." + random(5);

        testSender = createLogzioSender(token, type, drainTimeout, 10 * 1000,
                10 * 1000,  tasks,  true);


        testSender.send( createJsonMessage(loggerName, message1));
        testSender.send( createJsonMessage(loggerName, message2));
        sleepSeconds(drainTimeout  *3);

        mockListener.assertNumberOfReceivedMsgs(2);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);
        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
    }

    @Test
    public void multipleBufferDrains() throws Exception {
        String token = "tokenWohooToken";
        String type = random(8);
        String loggerName = "multipleBufferDrains";
        int drainTimeout = 2;

        String message1 = "Testing first drain - " + random(5);
        String message2 = "And the second drain" + random(5);

        LogzioSender testSender = createLogzioSender(token, type, drainTimeout, 10 * 1000,
                10 * 1000, tasks, false);

        testSender.send(createJsonMessage( loggerName, message1));
        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);

        testSender.send(createJsonMessage(loggerName, message2));
        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(2);
        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
    }

    @Test
    public void longDrainTimeout() throws Exception {
        String token = "soTestingIsSuperImportant";
        String type = random(8);
        String loggerName = "longDrainTimeout";
        int drainTimeout = 10;

        String message1 = "Sending one log - " + random(5);
        String message2 = "And one more important one - " + random(5);

        LogzioSender testSender = createLogzioSender(token, type, drainTimeout, 10 * 1000,
                10 * 1000, tasks, false);

        testSender.send(createJsonMessage(loggerName, message1));
        testSender.send(createJsonMessage(loggerName, message2));

        mockListener.assertNumberOfReceivedMsgs(0);
        sleepSeconds(drainTimeout + 1);

        mockListener.assertNumberOfReceivedMsgs(2);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);
        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
    }

    @Test
    public void fsPercentDrop() throws Exception {
        String token = "droppingLogsDueToFSOveruse";
        String type = random(8);
        String loggerName = "fsPercentDrop";
        int drainTimeoutSec = 1;
        File tempDirectoryThatWillBeInTheSameFsAsTheBuffer = TestEnvironment.createTempDirectory();
        tempDirectoryThatWillBeInTheSameFsAsTheBuffer.deleteOnExit();
        String message1 = "First log that will be dropped - " + random(5);
        String message2 = "And a second drop - " + random(5);
        setZeroThresholdBuffer();
        LogzioSender testSender = createLogzioSender(token, type, drainTimeoutSec, 10 * 1000,
                10 * 1000, tasks, false);

        // verify the thread that checks for space made at least one check
        sleepSeconds(2 * drainTimeoutSec);
        testSender.send(createJsonMessage(loggerName, message1));
        testSender.send(createJsonMessage(loggerName, message2));
        sleepSeconds(2 * drainTimeoutSec);
        mockListener.assertNumberOfReceivedMsgs(0);
        tempDirectoryThatWillBeInTheSameFsAsTheBuffer.delete();
    }

    @Test
    public void serverCrash() throws Exception {
        String token = "nowWeWillCrashTheServerAndRecover";
        String type = random(8);
        String loggerName = "serverCrash";
        int drainTimeout = 1;

        String message1 = "Log before drop - " + random(5);
        String message2 = "Log during drop - " + random(5);
        String message3 = "Log after drop - " + random(5);

        LogzioSender testSender = createLogzioSender(token, type,  drainTimeout, 10 * 1000,
                10 * 1000, tasks, false);

        testSender.send(createJsonMessage(loggerName, message1));

        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);

        mockListener.stop();

        testSender.send(createJsonMessage(loggerName, message2));
        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1); // haven't changed - still 1

        mockListener.start();

        testSender.send(createJsonMessage(loggerName, message3));

        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(3);
        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
        mockListener.assertLogReceivedIs(message3, token, type, loggerName, LOGLEVEL);
    }

    @Test
    public void getTimeoutFromServer() throws Exception {
        String token = "gettingTimeoutFromServer";
        String type = random(8);
        String loggerName = "getTimeoutFromServer";
        int drainTimeout = 1;
        int serverTimeout = 2000;

        String message1 = "Log that will be sent - " + random(5);
        String message2 = "Log that would timeout and then being re-sent - " + random(5);

        int socketTimeout = serverTimeout / 2;

        LogzioSender testSender = createLogzioSender(token, type, drainTimeout, socketTimeout,
                serverTimeout, tasks, false);


        testSender.send(createJsonMessage(loggerName, message1));

        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);

        mockListener.setTimeoutMillis(serverTimeout);
        mockListener.setServerTimeoutMode(true);

        testSender.send(createJsonMessage(loggerName, message2));

        sleepSeconds((socketTimeout / 1000) * MAX_RETRIES_ATTEMPTS + retryTotalDelay());

        mockListener.assertNumberOfReceivedMsgs(1); // Stays the same

        mockListener.setServerTimeoutMode(false);

        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(2);

        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
    }

    private int retryTotalDelay() {
        int sleepBetweenRetry = INITIAL_WAIT_BEFORE_RETRY_MS / 1000;
        int totalSleepTime = 0;
        for (int i = 1; i < MAX_RETRIES_ATTEMPTS; i++) {
            totalSleepTime += sleepBetweenRetry;
            sleepBetweenRetry *= 2;
        }
        return totalSleepTime;

    }

    @Test
    public void getExceptionFromServer() throws Exception {
        String token = "gettingExceptionFromServer";
        String type = random(8);
        String loggerName = "getExceptionFromServer";
        int drainTimeout = 1;

        String message1 = "Log that will be sent - " +  random(5);
        String message2 = "Log that would get exception and be sent again - " + random(5);

        LogzioSender testSender = createLogzioSender(token, type, drainTimeout, 10 * 1000,
                10 * 1000, tasks, false);
        testSender.send(createJsonMessage(loggerName, message1));
        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);
        mockListener.setFailWithServerError(true);

        testSender.send(createJsonMessage(loggerName, message2));
        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1); // Haven't changed
        mockListener.setFailWithServerError(false);

        Thread.sleep(drainTimeout * 1000 * 2);

        mockListener.assertNumberOfReceivedMsgs(2);
        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
    }
}