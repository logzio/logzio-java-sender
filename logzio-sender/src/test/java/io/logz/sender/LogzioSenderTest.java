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
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executors;

import static io.logz.sender.LogzioTestSenderUtil.LOGLEVEL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author MarinaRazumovsky
 */

public class LogzioSenderTest {

    private final static Logger logger = LoggerFactory.getLogger(LogzioSenderTest.class);
    private MockLogzioBulkListener mockListener;

    private static final int INITIAL_WAIT_BEFORE_RETRY_MS = 2000;
    private static final int MAX_RETRIES_ATTEMPTS = 3;

    @Before
    public void startMockListener() throws Exception {
        mockListener = new MockLogzioBulkListener();
        mockListener.start();
    }

    @After
    public void stopMockListener() {
        if (mockListener !=null)
             mockListener.stop();
    }

    private void sleepSeconds(int seconds) {
        logger.info("Sleeping {} [sec]...", seconds);
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String random(int numberOfChars) {
        return UUID.randomUUID().toString().substring(0, numberOfChars-1);
    }

    public LogzioSender getTestLogzioSender(String token, String type, Integer drainTimeout,
                                             Integer fsPercentThreshold, String bufferDir, Integer socketTimeout, Integer serverTimeout, int port) throws IOException, LogzioParameterErrorException {
        if (bufferDir == null) {
            File tempDir = TestEnvironment.createTempDirectory();
            tempDir.deleteOnExit();
            bufferDir = tempDir.getAbsolutePath();
        }
        LogzioSender sender =  LogzioSender.getOrCreateSenderByType(token, type, drainTimeout,fsPercentThreshold, bufferDir,
                "http://" + mockListener.getHost() + ":" + port, socketTimeout, serverTimeout, true, new LogzioTestStatusReporter(logger), Executors.newScheduledThreadPool(2),30);
        sender.start();
        return sender;
    }

    @Test
    public void simpleAppending() throws Exception {
        String token = "aBcDeFgHiJkLmNoPqRsT";
        String type = "awesomeType2";
        String loggerName = "simpleAppending";
        int drainTimeout = 2;

        String message1 = "Testing.." + random(5);
        String message2 = "Warning test.." + random(5);

        LogzioSender testSender = getTestLogzioSender(token, type, drainTimeout, 98,
                null, 10 * 1000,10 * 1000, mockListener.getPort());

        testSender.send( LogzioTestSenderUtil.createJsonMessage(loggerName, message1));
        testSender.send( LogzioTestSenderUtil.createJsonMessage(loggerName, message2));
        sleepSeconds(drainTimeout  *3);

        mockListener.assertNumberOfReceivedMsgs(2);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);
        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
    }

    @Test
    public void multipleBufferDrains() throws Exception {
        String token = "tokenWohooToken";
        String type = "typoosh";
        String loggerName = "multipleBufferDrains";
        int drainTimeout = 2;

        String message1 = "Testing first drain - " + random(5);
        String message2 = "And the second drain" + random(5);

        LogzioSender testSender = getTestLogzioSender(token, type, drainTimeout, 98,
                null, 10 * 1000, 10 * 1000, mockListener.getPort());

        testSender.send(LogzioTestSenderUtil.createJsonMessage( loggerName, message1));
        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);

        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message2));
        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(2);
        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
    }

    @Test
    public void longDrainTimeout() throws Exception {
        String token = "soTestingIsSuperImportant";
        String type = "andItsImportantToChangeStuff";
        String loggerName = "longDrainTimeout";
        int drainTimeout = 10;

        String message1 = "Sending one log - " + random(5);
        String message2 = "And one more important one - " + random(5);

        LogzioSender testSender = getTestLogzioSender(token, type, drainTimeout, 98,
                null, 10 * 1000,10 * 1000, mockListener.getPort());
        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message1));
        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message2));

        mockListener.assertNumberOfReceivedMsgs(0);
        sleepSeconds(drainTimeout + 1);

        mockListener.assertNumberOfReceivedMsgs(2);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);
        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
    }

    @Test
    public void testLoggerCreatesDirectoryWhichDoesNotExists() throws Exception {
        String token = "nowWeWantToChangeTheBufferLocation";
        String type = "justTestingExistence";
        String loggerName = "changeBufferLocation";
        int drainTimeout = 10;
        File tempDirectory = TestEnvironment.createTempDirectory();

        String bufferDir = new File(tempDirectory, "dirWhichDoesNotExists").getAbsolutePath();
        String message1 = "Just sending something - " + random(5);
        File buffer = new File(bufferDir);
        assertFalse(buffer.exists());

        LogzioSender testSender = getTestLogzioSender(token, type, drainTimeout, 98,
                bufferDir, 10 * 1000,10 * 1000, mockListener.getPort());

        testSender.send( LogzioTestSenderUtil.createJsonMessage(loggerName, message1));
        assertTrue(buffer.exists());
        tempDirectory.delete();
    }

    @Test
    public void testLoggerCantWriteToEmptyDirectory() throws Exception {
        String token = "nowWeTestLoggerCantWriteToTmpDirectory";
        String type = "justTestingNoWriteDir";
        String loggerName = "changeBufferLocation";
        int drainTimeout = 10;
        File tempDirectory = new File(""+File.separator);
        tempDirectory.setWritable(false);
        String bufferDir = tempDirectory.getAbsolutePath();
        String message1 = "Just sending something - " + random(5);
        try {
            LogzioSender testSender = getTestLogzioSender(token, type, drainTimeout, 98,
                    bufferDir, 10 * 1000, 10 * 1000, mockListener.getPort());
        } catch(LogzioParameterErrorException e) {
            assertTrue(e.getMessage().contains(bufferDir));
        }
        assertTrue(tempDirectory.exists());
        tempDirectory.delete();
    }

    @Test
    public void fsPercentDrop() throws Exception {
        String token = "droppingLogsDueToFSOveruse";
        String type = "droppedType";
        String loggerName = "fsPercentDrop";
        int drainTimeoutSec = 1;
        File tempDirectoryThatWillBeInTheSameFsAsTheBuffer = TestEnvironment.createTempDirectory();
        tempDirectoryThatWillBeInTheSameFsAsTheBuffer.deleteOnExit();
        int fsPercentDrop = 100 - ((int) (((double) tempDirectoryThatWillBeInTheSameFsAsTheBuffer.getUsableSpace() /
                tempDirectoryThatWillBeInTheSameFsAsTheBuffer.getTotalSpace()) * 100)) - 1;
        String message1 = "First log that will be dropped - " + random(5);
        String message2 = "And a second drop - " + random(5);
        LogzioSender testSender = getTestLogzioSender(token, type, drainTimeoutSec, fsPercentDrop,
                null, 10 * 1000, 10 * 1000, mockListener.getPort());

        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message1));
        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message2));
        sleepSeconds(2 * drainTimeoutSec);
        mockListener.assertNumberOfReceivedMsgs(0);
        tempDirectoryThatWillBeInTheSameFsAsTheBuffer.delete();
    }

    @Test
    public void serverCrash() throws Exception {
        String token = "nowWeWillCrashTheServerAndRecover";
        String type = "crashingType";
        String loggerName = "serverCrash";
        int drainTimeout = 1;

        String message1 = "Log before drop - " + random(5);
        String message2 = "Log during drop - " + random(5);
        String message3 = "Log after drop - " + random(5);

        LogzioSender testSender = getTestLogzioSender(token, type,  drainTimeout, 98,
                null, 10 * 1000, 10 * 1000, mockListener.getPort());

        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message1));

        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);

        mockListener.stop();

        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message2));
        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1); // haven't changed - still 1

        mockListener.start();

        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message3));

        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(3);
        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
        mockListener.assertLogReceivedIs(message3, token, type, loggerName, LOGLEVEL);
    }

    @Test
    public void getTimeoutFromServer() throws Exception {
        String token = "gettingTimeoutFromServer";
        String type = "timeoutType";
        String loggerName = "getTimeoutFromServer";
        int drainTimeout = 1;
        int serverTimeout = 2000;

        String message1 = "Log that will be sent - " + random(5);
        String message2 = "Log that would timeout and then being re-sent - " + random(5);

        int socketTimeout = serverTimeout / 2;

        LogzioSender testSender = getTestLogzioSender(token, type, drainTimeout, 98,
                null, socketTimeout, serverTimeout,  mockListener.getPort());


        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message1));

        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);

        mockListener.setTimeoutMillis(serverTimeout);
        mockListener.setServerTimeoutMode(true);

        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message2));

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
        String type = "exceptionType";
        String loggerName = "getExceptionFromServer";
        int drainTimeout = 1;

        String message1 = "Log that will be sent - " +  random(5);
        String message2 = "Log that would get exception and be sent again - " + random(5);

        LogzioSender testSender = getTestLogzioSender(token, type, drainTimeout, 98,
                null, 10*1000, 10*1000,  mockListener.getPort());
        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message1));
        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1);
        mockListener.assertLogReceivedIs(message1, token, type, loggerName, LOGLEVEL);
        mockListener.setFailWithServerError(true);

        testSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, message2));
        sleepSeconds(2 * drainTimeout);

        mockListener.assertNumberOfReceivedMsgs(1); // Haven't changed
        mockListener.setFailWithServerError(false);

        Thread.sleep(drainTimeout * 1000 * 2);

        mockListener.assertNumberOfReceivedMsgs(2);
        mockListener.assertLogReceivedIs(message2, token, type, loggerName, LOGLEVEL);
    }

}