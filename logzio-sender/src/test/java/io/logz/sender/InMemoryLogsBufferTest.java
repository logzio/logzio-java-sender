package io.logz.sender;

import io.logz.sender.exceptions.LogzioParameterErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

public class InMemoryLogsBufferTest extends LogzioSenderTest {
    private final static Logger logger = LoggerFactory.getLogger(LogzioSenderTest.class);
    private boolean zeroThresholdBuffer = false;

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

        int bufferThreshold = zeroThresholdBuffer ? 0 : 100 * 1024 * 1024;

        LogzioSender logzioSender = LogzioSender
                .builder()
                .setDebug(false)
                .setTasksExecutor(tasks)
                .setDrainTimeout(drainTimeout)
                .setHttpsRequestConfiguration(httpsRequestConfiguration)
                .withInMemoryLogsBuffer()
                    .setBufferThreshold(bufferThreshold)
                .endInMemoryLogsBuffer()
                .setReporter(logy)
                .build();

        logzioSender.start();
        return logzioSender;
    }

    @Override
    protected void setZeroThresholdBuffer() throws LogzioParameterErrorException {
        this.zeroThresholdBuffer = true;
    }
}
