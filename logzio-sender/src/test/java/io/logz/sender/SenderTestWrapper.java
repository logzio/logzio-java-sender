package io.logz.sender;

import com.google.gson.JsonObject;
import io.logz.test.BaseTest;
import io.logz.test.TestEnvironment;
import io.logz.test.TestWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * Created by MarinaRazumovsky on 27/12/2016.
 */
public class SenderTestWrapper implements TestWrapper {

    protected final static Logger logger = LoggerFactory.getLogger(SenderTestWrapper.class);

    private LogzioSender logzioSender;
    private String loggerName;

    SenderTestWrapper(String token, String type, String loggerName, Integer drainTimeout, Integer fsPercentThreshold, String bufferDir, Integer socketTimeout, int port) {
        if (bufferDir == null) {
            File tempDir = TestEnvironment.createTempDirectory();
            tempDir.deleteOnExit();
            bufferDir = tempDir.getAbsolutePath();
        }
        this.loggerName = loggerName;
        logzioSender = LogzioSender.getOrCreateSenderByType(token, type, drainTimeout,fsPercentThreshold, bufferDir,
                "http://" + BaseTest.LISTENER_ADDRESS + ":" + port, socketTimeout, 10*1000, true, new SenderStatusReporter(), Executors.newScheduledThreadPool(2),30);
        logzioSender.start();
    }

    private JsonObject createJsonObject(String logname, String message) {
        JsonObject obj = new JsonObject();
        obj.addProperty("message", message);
        obj.addProperty("logger", logname);
        return obj;
    }


    @Override
    public Optional info( String message) {
        logzioSender.send(createJsonObject(loggerName, message));
        return Optional.empty();
    }

    @Override
    public Optional warn( String message) {
        logzioSender.send(createJsonObject(loggerName,message));
        return Optional.empty();
    }

    @Override
    public Optional error( String message) {
        logzioSender.send(createJsonObject(loggerName, message));
        return Optional.empty();
    }

    @Override
    public Optional info( String message, Throwable exc) {
        logzioSender.send(createJsonObject(loggerName,message+" exception: "+exc.getMessage()));
        return Optional.empty();
    }

    public Optional info(Marker market, String message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop() {
        logger.info("Stop sender");
        if ( logzioSender != null )
            logzioSender.stop();
    }

    private class SenderStatusReporter implements LogzioStatusReporter {

        @Override
        public void error(String msg) {
            System.out.println("ERROR: " + msg);
        }

        @Override
        public void error(String msg, Throwable e) {
            System.out.println("ERROR: " + msg);
            e.printStackTrace();
        }

        @Override
        public void warning(String msg) {
            System.out.println("WARNING: " + msg);
        }

        @Override
        public void warning(String msg, Throwable e) {
            System.out.println("WARNING: " + msg);
            e.printStackTrace();
        }

        @Override
        public void info(String msg) {
            System.out.println("INFO: " + msg);
        }

        @Override
        public void info(String msg, Throwable e) {
            System.out.println("INFO: " + msg);
            e.printStackTrace();
        }
    }

}
