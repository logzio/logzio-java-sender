package io.logz.sender;

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
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author MarinaRazumovsky
 */

public class LogzioLongRunningTests {

    private final static Logger logger = LoggerFactory.getLogger(LogzioLongRunningTests.class);
    private MockLogzioBulkListener mockListener;

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

    public LogzioSender getTestLogzioSender(String token, String type, Integer drainTimeout, int gcInterval, int port) throws Exception {
        File tempDir = TestEnvironment.createTempDirectory();
        tempDir.deleteOnExit();
        LogzioSender sender =  LogzioSender.getOrCreateSenderByType(token, type, drainTimeout, 98, tempDir,
                "http://" + mockListener.getHost() + ":" + port, 10*1000, 10*1000, true, new LogzioTestStatusReporter(logger), Executors.newScheduledThreadPool(2),gcInterval);
        sender.start();
        return sender;
    }

    @Test
    public void testDeadLock() throws Exception {
        String token = "aBcDeFgHiJkLmNoPqRsU";
        String type = "awesomeType";
        String loggerName = "deadlockLogger";
        int drainTimeout = 1;
        Integer gcInterval = 1;
        LogzioSender logzioSender = getTestLogzioSender(token, type, drainTimeout, gcInterval, mockListener.getPort());


        List<Thread> threads = new ArrayList<>();
        try {
            int threadCount = 10;
            CountDownLatch countDownLatch = new CountDownLatch(threadCount);
            final int msgCount = 100000000;
            for (int j = 1; j < threadCount; j++) {
                Thread thread = new Thread(() -> {
                    for (int i = 1; i <= msgCount; i++) {
                        logzioSender.send(LogzioTestSenderUtil.createJsonMessage(loggerName, "Hello i"));
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
