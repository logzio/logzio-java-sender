package io.logz.sender;

import com.google.gson.JsonObject;
import io.logz.sender.exceptions.LogzioParameterErrorException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.logz.sender.LogzioTestSenderUtil.createJsonMessage;

public class InMemoryQueueTest extends LogzioSenderTest {
    private final static Logger logger = LoggerFactory.getLogger(LogzioSenderTest.class);
    private boolean zeroThresholdBuffer = false;
    private int capacityInBytes = 100 * 1024 * 1024;

    @Override
    protected LogzioSender createLogzioSender(String token, String type, Integer drainTimeout,
                                              Integer socketTimeout, Integer serverTimeout,
                                              ScheduledExecutorService tasks, boolean compressRequests)
            throws LogzioParameterErrorException {

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

        capacityInBytes = zeroThresholdBuffer ? 0 : capacityInBytes;

        LogzioSender logzioSender = LogzioSender
                .builder()
                .setDebug(false)
                .setTasksExecutor(tasks)
                .setDrainTimeoutSec(drainTimeout)
                .setHttpsRequestConfiguration(httpsRequestConfiguration)
                .withInMemoryQueue()
                    .setCapacityInBytes(capacityInBytes)
                .endInMemoryQueue()
                .setReporter(logy)
                .build();

        logzioSender.start();
        return logzioSender;
    }

    @Override
    protected void setZeroThresholdBuffer() {
        this.zeroThresholdBuffer = true;
    }

    private void setCapacityInBytes(int capacityInBytes) {
        this.capacityInBytes = capacityInBytes;
    }

    @Test
    public void checkCrossCapacityInBytes() throws LogzioParameterErrorException {
        String token = "checkCrossCapacityInBytes";
        String type = random(8);
        String loggerName = "checkCrossCapacityInBytesName";
        int drainTimeout = 1;

        String message = "Log before drop - " + random(5);
        JsonObject log = createJsonMessage(loggerName, message);
        int logSize = log.getAsByte()
        ScheduledExecutorService tasks = Executors.newScheduledThreadPool(3);
        setCapacityInBytes(20);
        LogzioSender testSender = createLogzioSender(token, type,  drainTimeout, 10 * 1000,
                10 * 1000, tasks, false);

        testSender.send(byte);

        sleepSeconds(2 * drainTimeout);

    }
}
