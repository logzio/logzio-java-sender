package io.logz.sender;

import io.logz.sender.exceptions.LogzioParameterErrorException;
import io.logz.test.TestEnvironment;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.logz.sender.LogzioTestSenderUtil.createJsonMessage;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiskQueueTest extends LogzioSenderTest{
    private final static int FS_PERCENT_THRESHOLD = 98;
    private final static Logger logger = LoggerFactory.getLogger(LogzioSenderTest.class);
    private boolean zeroThresholdBuffer = false;
    private File bufferDir;

    @Override
    protected LogzioSender createLogzioSender(String token, String type, Integer drainTimeout,
                                              Integer socketTimeout, Integer serverTimeout,
                                              ScheduledExecutorService tasks,
                                              boolean compressRequests) throws LogzioParameterErrorException {

        LogzioTestStatusReporter logy = new LogzioTestStatusReporter(logger);
        HttpsRequestConfiguration httpsRequestConfiguration = HttpsRequestConfiguration
                .builder()
                .setCompressRequests(compressRequests)
                .setConnectTimeout(serverTimeout)
                .setSocketTimeout(socketTimeout)
                .setLogzioToken(token)
                .setLogzioType(type)
                .setLogzioListenerUrl("http://" + getMockListenerHost() + ":" + getMockListenerPort())
                .build();

        if (bufferDir == null) {
            bufferDir = TestEnvironment.createTempDirectory();
            bufferDir.deleteOnExit();
        }

        int fsPercentThreshold = zeroThresholdBuffer ? 0 : FS_PERCENT_THRESHOLD;

        LogzioSender logzioSender = LogzioSender
                .builder()
                .setDebug(false)
                .setTasksExecutor(tasks)
                .setDrainTimeout(drainTimeout)
                .setReporter(logy)
                .setHttpsRequestConfiguration(httpsRequestConfiguration)
                .WithDiskMemoryQueue()
                    .setBufferDir(bufferDir)
                    .setFsPercentThreshold(fsPercentThreshold)
                    .setCheckDiskSpaceInterval(1000)
                .EndDiskQueue()
                .build();

        logzioSender.start();
        return logzioSender;
    }

    @Override
    protected void setZeroThresholdBuffer() throws LogzioParameterErrorException {
        zeroThresholdBuffer = true;
    }

    private void setBufferDir(File bufferDir) {
        this.bufferDir = bufferDir;
    }

    @Test
    public void testLoggerCantWriteToEmptyDirectory() {
        String token = "nowWeTestLoggerCantWriteToTmpDirectory";
        String type = "justTestingNoWriteDir";
        String loggerName = "changeBufferLocation";
        int drainTimeout = 10;
        File tempDirectory = new File("" + File.separator);
        String message1 = "Just sending something - " + random(5);
        ScheduledExecutorService tasks = Executors.newScheduledThreadPool(3);
        try {
            setBufferDir(tempDirectory);
            LogzioSender testSender = createLogzioSender(token, type, drainTimeout, 10 * 1000,
                    10 * 1000, tasks, false);
        } catch(LogzioParameterErrorException e) {
            assertTrue(e.getMessage().contains(tempDirectory.getAbsolutePath()));
        }
        assertTrue(tempDirectory.exists());
        tempDirectory.delete();
        tasks.shutdownNow();
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
        ScheduledExecutorService tasks = Executors.newScheduledThreadPool(3);

        assertFalse(bufferDir.exists());
        setBufferDir(bufferDir);
        LogzioSender testSender = createLogzioSender(token, type, drainTimeout,
                10 * 1000, 10 * 1000, tasks, false);

        testSender.send( createJsonMessage(loggerName, message1));
        assertTrue(bufferDir.exists());
        tempDirectory.delete();
        tasks.shutdownNow();
    }
}
