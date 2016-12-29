package io.logz.sender;

import io.logz.test.LongRunningTests;
import io.logz.test.TestWrapper;

/**
 * Created by MarinaRazumovsky on 27/12/2016.
 */
public class SenderLongRunningTests extends LongRunningTests {

    @Override
    public TestWrapper getTestSenderWrapper(String token, String type, String loggerName, Integer drainTimeout, Integer fsPercentThreshold, String bufferDir, Integer socketTimeout, boolean addHostname, String additionalFields, int gcPersistedQueueFilesIntervalSeconds, int port) {
        return new SenderTestWrapper(token,type,loggerName,drainTimeout,fsPercentThreshold,bufferDir,socketTimeout,port);

    }
}
