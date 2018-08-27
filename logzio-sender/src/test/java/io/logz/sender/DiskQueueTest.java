package io.logz.sender;

import io.logz.sender.exceptions.LogzioParameterErrorException;
import io.logz.test.TestEnvironment;
import org.junit.Test;

import java.io.File;

import static io.logz.sender.LogzioTestSenderUtil.createJsonMessage;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiskQueueTest extends LogzioSenderTest{
    private static final int FS_PERCENT_THRESHOLD = 98;

    @Override
    protected LogzioSender createLogzioSender(String token, String type, Integer drainTimeout,
                                              Integer socketTimeout, Integer serverTimeout,
                                              LogzioLogsBufferInterface buffer,
                                              boolean compressRequests) throws LogzioParameterErrorException {

        HttpsRequestConfiguration httpsRequestConfiguration = HttpsRequestConfiguration
                .builder()
                .setCompressRequests(compressRequests)
                .setConnectTimeout(serverTimeout)
                .setSocketTimeout(socketTimeout)
                .setLogzioToken(token)
                .setLogzioType(type)
                .setLogzioListenerUrl("http://" + getMockListenerHost() + ":" + getMockListenerPort())
                .build();

        LogzioLogsBufferInterface logsBuffer = buffer == null ?
                createDiskQueue(null, FS_PERCENT_THRESHOLD)
                : buffer;

        LogzioSender logzioSender = LogzioSender
                .builder()
                .setDebug(true)
                .setDrainTimeout(drainTimeout)
                .setReporter(getReporter())
                .setHttpsRequestConfiguration(httpsRequestConfiguration)
                .setLogsBuffer(logsBuffer)
                .build();

        logzioSender.start();
        return logzioSender;
    }

    private LogzioLogsBufferInterface createDiskQueue(File bufferDir, int fsPercentThreshold) throws LogzioParameterErrorException {
        if (bufferDir == null) {
            bufferDir = TestEnvironment.createTempDirectory();
            bufferDir.deleteOnExit();
        }

        return DiskQueue
                .builder()
                .setBufferDir(bufferDir)
                .setReporter(getReporter())
                .setFsPercentThreshold(fsPercentThreshold)
                .build();
    }

    @Override
    protected LogzioLogsBufferInterface createZeroThresholdBuffer() throws LogzioParameterErrorException {
        return createDiskQueue(null, 0);
    }

    @Test
    public void testLoggerCantWriteToEmptyDirectory() {
        String token = "nowWeTestLoggerCantWriteToTmpDirectory";
        String type = "justTestingNoWriteDir";
        String loggerName = "changeBufferLocation";
        int drainTimeout = 10;
        File tempDirectory = new File("" + File.separator);
        String message1 = "Just sending something - " + random(5);
        try {
            LogzioLogsBufferInterface buffer = createDiskQueue(tempDirectory, FS_PERCENT_THRESHOLD);
            LogzioSender testSender = createLogzioSender(token, type, drainTimeout, 10 * 1000,
                    10 * 1000, buffer, false);
        } catch(LogzioParameterErrorException e) {
            assertTrue(e.getMessage().contains(tempDirectory.getAbsolutePath()));
        }
        assertTrue(tempDirectory.exists());
        tempDirectory.delete();
    }

    @Test
    public void testLoggerCreatesDirectoryWhichDoesNotExists() throws Exception {
        String token = "nowWeWantToChangeTheBufferLocation";
        String type = "justTestingExistence";
        String loggerName = "changeBufferLocation";
        int drainTimeout = 10;
        File tempDirectory = TestEnvironment.createTempDirectory();

        File bufferDir = new File(tempDirectory, "dirWhichDoesNotExists");
        String message1 = "Just sending something - " + random(5);

        assertFalse(bufferDir.exists());
        LogzioLogsBufferInterface buffer = createDiskQueue(bufferDir, FS_PERCENT_THRESHOLD);
        LogzioSender testSender = createLogzioSender(token, type, drainTimeout,
                10 * 1000, 10 * 1000, buffer, false);

        testSender.send( createJsonMessage(loggerName, message1));
        assertTrue(bufferDir.exists());
        tempDirectory.delete();
    }
}
