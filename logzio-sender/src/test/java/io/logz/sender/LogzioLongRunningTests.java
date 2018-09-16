package io.logz.sender;

import io.logz.sender.LogzioSender.Builder;
import io.logz.sender.exceptions.LogzioParameterErrorException;
import io.logz.test.MockLogzioBulkListener;
import io.logz.test.TestEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.logz.sender.LogzioTestSenderUtil.createJsonMessage;
import static org.assertj.core.api.Assertions.assertThat;

public class LogzioLongRunningTests {
    private final static Logger logger = LoggerFactory.getLogger(LogzioLongRunningTests.class);
    private MockLogzioBulkListener mockListener;
    private ScheduledExecutorService tasks;

    @Before
    public void preTest() throws Exception {
        mockListener = new MockLogzioBulkListener();
        mockListener.start();
        tasks = Executors.newScheduledThreadPool(3);
    }

    @After
    public void stopMockListener() {
        if (mockListener != null)
            mockListener.stop();
        tasks.shutdownNow();
    }

    private HttpsRequestConfiguration getHttpsRequestConfiguration(String token, String type, Integer port) throws LogzioParameterErrorException {
        return HttpsRequestConfiguration
                .builder()
                .setCompressRequests(true)
                .setLogzioToken(token)
                .setLogzioType(type)
                .setLogzioListenerUrl("http://" + mockListener.getHost() + ":" + port)
                .build();
    }

    private Builder getLogzioSenderBuilder(int drainTimeout, SenderStatusReporter reporter, HttpsRequestConfiguration conf) {
        return LogzioSender
                .builder()
                .setDrainTimeoutSec(drainTimeout)
                .setReporter(reporter)
                .setHttpsRequestConfiguration(conf)
                .setTasksExecutor(tasks);
    }

    @Test
    public void testDeadLock() throws Exception {
        String token = "aBcDeFgHiJkLmNoPqRsU";
        String type = "awesomeType";
        String loggerName = "deadlockLogger";
        int drainTimeout = 1;
        Integer gcInterval = 1;
        final int msgCount = 100000000;
        File tempDir = TestEnvironment.createTempDirectory();
        tempDir.deleteOnExit();
        SenderStatusReporter reporter = new LogzioTestStatusReporter(logger);
        HttpsRequestConfiguration conf = getHttpsRequestConfiguration(token, type, mockListener.getPort());
        Builder logzioSenderBuilder = getLogzioSenderBuilder(drainTimeout, reporter, conf);
        LogzioSender logzioSender = logzioSenderBuilder
                .withDiskQueue()
                .setGcPersistedQueueFilesIntervalSeconds(gcInterval)
                .setBufferDir(tempDir)
                .endDiskQueue()
                .build();
        logzioSender.start();
        sendLogs(loggerName, logzioSender, msgCount);
    }

    @Test
    public void testInMemoryLongRun() throws Exception {
        String token = "aBcDeFgHiJkLmNoPqRsUInMemoryLongRun";
        String type = "awesomeTypeInMemory";
        String loggerName = "InMemoryLongRun";
        int drainTimeout = 1;
        final int msgCount = 100000;
        HttpsRequestConfiguration conf = getHttpsRequestConfiguration(token, type, mockListener.getPort());
        SenderStatusReporter reporter = new LogzioTestStatusReporter(logger);
        Builder logzioSenderBuilder = getLogzioSenderBuilder(drainTimeout, reporter, conf);
        LogzioSender logzioSender = logzioSenderBuilder
                .withInMemoryQueue()
                .setCapacityInBytes(InMemoryQueue.DONT_LIMIT_BUFFER_SPACE)
                .endInMemoryQueue()
                .build();
        logzioSender.start();
        sendLogs(loggerName, logzioSender, msgCount);
    }

    private void sendLogs(String loggerName, LogzioSender logzioSender, int msgCount) throws InterruptedException {
        List<Thread> threads = new ArrayList<>();
        try {
            int threadCount = 10;
            CountDownLatch countDownLatch = new CountDownLatch(threadCount);
            for (int j = 1; j < threadCount; j++) {
                Thread thread = new Thread(() -> {
                    for (int i = 1; i <= msgCount; i++) {
                        logzioSender.send(createJsonMessage(loggerName, "Hello " + i));
                        if (Thread.interrupted()) {
                            logger.info("Stopping thread - interrupted");
                            break;
                        }
                    }
                    countDownLatch.countDown();
                });
                thread.start();
                threads.add(thread);
            }

            countDownLatch.await(100, TimeUnit.SECONDS);

            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            long[] threadIds = bean.findDeadlockedThreads(); // Returns null if no threads are deadlocked.

            if (threadIds != null) {
                ThreadInfo[] infos = bean.getThreadInfo(threadIds);

                for (ThreadInfo info : infos) {
                    System.out.println("Locked thread: "+ info);
                }
            } else {
                logger.info("No deadlocked threads");
            }

            assertThat(threadIds).isNull();

        } finally {
            threads.forEach(Thread::interrupt);
        }
    }
}
