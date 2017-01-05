package io.logz.sender;

import com.google.gson.JsonObject;
import org.slf4j.Logger;

import java.util.UUID;

/**
 * @author MarinaRazumovsky
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

}
