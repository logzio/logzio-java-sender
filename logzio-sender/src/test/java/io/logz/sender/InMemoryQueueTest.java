package io.logz.sender;

import io.logz.sender.exceptions.LogzioParameterErrorException;

public class InMemoryQueueTest extends LogzioSenderTest {

    @Override
    protected LogzioSender createLogzioSender(String token, String type, Integer drainTimeout,
                                              Integer socketTimeout, Integer serverTimeout,
                                              LogzioLogsBufferInterface buffer, boolean compressRequests)
            throws LogzioParameterErrorException {

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
                InMemoryQueue
                        .builder()
                        .setReporter(getReporter())
                        .build()
                : buffer;

        LogzioSender logzioSender = LogzioSender
                .builder()
                .setDebug(true)
                .setDrainTimeout(drainTimeout)
                .setHttpsRequestConfiguration(httpsRequestConfiguration)
                .setLogsBuffer(logsBuffer)
                .setReporter(getReporter())
                .build();

        logzioSender.start();
        return logzioSender;
    }

    @Override
    protected LogzioLogsBufferInterface createZeroThresholdBuffer() throws LogzioParameterErrorException {
        return InMemoryQueue
                .builder()
                .setReporter(getReporter())
                .setBufferThreshold(0)
                .build();
    }
}
