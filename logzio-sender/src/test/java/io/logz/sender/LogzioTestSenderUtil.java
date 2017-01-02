package io.logz.sender;

import com.google.gson.JsonObject;
import org.slf4j.Logger;

/**
 * Created by MarinaRazumovsky on 27/12/2016.
 */
public class LogzioTestSenderUtil {

    public static final String LOGLEVEL = "info";

    public static JsonObject createJsonMessage(String loggerName, String message){
        JsonObject obj = new JsonObject();
        obj.addProperty("message", message);
        obj.addProperty("timestamp", System.currentTimeMillis());
        obj.addProperty("loglevel", LOGLEVEL);
        obj.addProperty("logger", loggerName);
        return obj;
    }


    public static class StatusReporter implements LogzioStatusReporter {

        private Logger logger;

        public StatusReporter(Logger logger){
            this.logger = logger;
        }

        @Override
        public void error(String msg) {
            logger.error(msg);
        }

        @Override
        public void error(String msg, Throwable throwable) {
            logger.error(msg, throwable);
        }

        @Override
        public void warning(String msg) {
            logger.warn(msg);
        }

        @Override
        public void warning(String msg, Throwable throwable) {
            logger.warn(msg,throwable);
        }

        @Override
        public void info(String msg) {
            logger.info(msg);
        }

        @Override
        public void info(String msg, Throwable throwable) {
            logger.info(msg,throwable);
        }
    }
}
