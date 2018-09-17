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

public class DiskQueueTest extends LogzioSenderTest {
    private final static int FS_PERCENT_THRESHOLD = 98;
    private final static Logger logger = LoggerFactory.getLogger(LogzioSenderTest.class);
    private boolean zeroThresholdQueue = false;
    private File queueDir;

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

        if (queueDir == null) {
            queueDir = TestEnvironment.createTempDirectory();
            queueDir.deleteOnExit();
        }

        int fsPercentThreshold = zeroThresholdQueue ? 0 : FS_PERCENT_THRESHOLD;

        LogzioSender logzioSender = LogzioSender
                .builder()
                .setDebug(false)
                .setTasksExecutor(tasks)
                .setDrainTimeoutSec(drainTimeout)
                .setReporter(logy)
                .setHttpsRequestConfiguration(httpsRequestConfiguration)
                .withDiskQueue()
                    .setQueueDir(queueDir)
                    .setFsPercentThreshold(fsPercentThreshold)
                    .setCheckDiskSpaceInterval(1000)
                .endDiskQueue()
                .build();
        logzioSender.start();
        return logzioSender;
    }

    @Override
    protected void setZeroThresholdQueue() {
        zeroThresholdQueue = true;
    }

    private void setQueueDir(File queueDir) {
        this.queueDir = queueDir;
    }

    @Test
    public void testSenderCantWriteToEmptyDirectory() {
        String token = "nowWeTestLoggerCantWriteToTmpDirectory";
        String type = "justTestingNoWriteDir";
        int drainTimeout = 10;
        File tempDirectory = new File("" + File.separator);
        ScheduledExecutorService tasks = Executors.newScheduledThreadPool(3);
        try {
            setQueueDir(tempDirectory);
            LogzioSender testSender = createLogzioSender(token, type, drainTimeout, 10 * 1000,
                    10 * 1000, tasks, false);
            throw new LogzioParameterErrorException("Should not reach here", "fail");
        } catch(LogzioParameterErrorException e) {
            assertTrue(e.getMessage().contains(tempDirectory.getAbsolutePath()));
        }
        assertTrue(tempDirectory.exists());
        tempDirectory.delete();
        tasks.shutdownNow();
    }

    @Test

    public void testSenderCreatesDirectoryWhichDoesNotExists() throws Exception {
        String token = "nowWeWantToChangeTheQueueLocation";
        String type = "justTestingExistence";
        String loggerName = "changeQueueLocation";
        int drainTimeout = 10;
        File tempDirectory = TestEnvironment.createTempDirectory();
        File queueDir = new File(tempDirectory, "dirWhichDoesNotExists");
        String message1 = "Just sending something - " + random(5);
        ScheduledExecutorService tasks = Executors.newScheduledThreadPool(3);

        assertFalse(queueDir.exists());
        setQueueDir(queueDir);
        LogzioSender testSender = createLogzioSender(token, type, drainTimeout,
                10 * 1000, 10 * 1000, tasks, false);

        testSender.send( createJsonMessage(loggerName, message1));
        assertTrue(queueDir.exists());
        tempDirectory.delete();
        tasks.shutdownNow();
    }
}
