package io.logz.sender;


import io.logz.test.SendTest;
import io.logz.test.TestWrapper;

public class SenderSentTest extends SendTest {


    public TestWrapper getTestSenderWrapper(String token, String type, String loggerName, Integer drainTimeout, Integer fsPercentThreshold, String bufferDir, Integer socketTimeout, boolean addHostname, String additionalFields, int gcPersistedQueueFilesIntervalSeconds, int port) {
        return new SenderTestWrapper(token,type,loggerName,drainTimeout,fsPercentThreshold,bufferDir,socketTimeout,port);

    }
}