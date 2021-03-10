package io.logz.sender;

import com.google.gson.JsonObject;
import com.jayway.jsonpath.JsonPath;
import io.logz.sender.LogzioSender.Builder;
import io.logz.sender.exceptions.LogzioParameterErrorException;
import io.logz.test.TestEnvironment;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.logz.sender.LogzioTestSenderUtil.createJsonMessage;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiskQueueTest extends LogzioSenderTest {
    private final static int FS_PERCENT_THRESHOLD = 98;
    private File queueDir;

    @Override
    protected Builder getLogzioSenderBuilder(String token, String type, Integer drainTimeout,
                                              Integer socketTimeout, Integer serverTimeout,
                                              ScheduledExecutorService tasks,
                                              boolean compressRequests) throws LogzioParameterErrorException {
        Builder logzioSenderBuilder = super.getLogzioSenderBuilder(token, type, drainTimeout,
                socketTimeout, serverTimeout, tasks, compressRequests);

        if (queueDir == null) {
            queueDir = TestEnvironment.createTempDirectory();
            queueDir.deleteOnExit();
        }
        return logzioSenderBuilder
                .withDiskQueue()
                    .setQueueDir(queueDir)
                    .setFsPercentThreshold(FS_PERCENT_THRESHOLD)
                    .setCheckDiskSpaceInterval(1000)
                .endDiskQueue();
    }

    @Override
    protected void setZeroThresholdQueue(LogzioSender.Builder logzioSenderBuilder) {
        setFsPercentThreshold(logzioSenderBuilder, 0);
    }

    private void setFsPercentThreshold(LogzioSender.Builder logzioSenderBuilder, int fsPercentThreshold) {
        logzioSenderBuilder
                .withDiskQueue()
                    .setFsPercentThreshold(fsPercentThreshold)
                .endDiskQueue();
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
            Builder testSenderBuilder = getLogzioSenderBuilder(token, type, drainTimeout, 10 * 1000,
                    10 * 1000, tasks, false);
            LogzioSender testSender = createLogzioSender(testSenderBuilder);
            throw new LogzioParameterErrorException("Should not reach here", "fail");
        } catch(LogzioParameterErrorException e) {
            assertTrue(e.getMessage().contains(tempDirectory.getAbsolutePath()));
        }
        assertTrue(tempDirectory.exists());
        tempDirectory.delete();
        tasks.shutdownNow();
    }


//    @Test
//    public void simpleFiltering() throws Exception {
//        String token = "aBcDeFgHiJkLmNoPqRsT";
//        String type = random(8);
//        String loggerName = "simpleAppending";
//        int drainTimeout = 2;
//
////        JsonObject json = new JsonObject();
////        json.addProperty("type", "something");
////        json.addProperty("count", 5);
////        json.addProperty("msg", "all by myself");
//        String json = "{\"menu\": {\n" +
//                "  \"id\": \"file\",\n" +
//                "  \"value\": \"File\",\n" +
//                "  \"popup\": {\n" +
//                "    \"menuitem\": [\n" +
//                "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
//                "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
//                "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
//                "    ]\n" +
//                "  }\n" +
//                "}}";
//        String json2 = "{\"kaki\":\"yarok\"}";
//
//        System.out.println(JsonPath.parse("[" + json + "]").read("$..[?(@.id)]").toString());
//
//    }

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
        Builder testSenderBuilder = getLogzioSenderBuilder(token, type, drainTimeout,
                10 * 1000, 10 * 1000, tasks, false);
        LogzioSender testSender = createLogzioSender(testSenderBuilder);

        testSender.send( createJsonMessage(loggerName, message1));
        assertTrue(queueDir.exists());
        tempDirectory.delete();
        tasks.shutdownNow();
    }
}
