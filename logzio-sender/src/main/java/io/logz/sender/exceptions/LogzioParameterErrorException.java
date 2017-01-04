package io.logz.sender.exceptions;

/**
 * @author MarinaRazumovsky
 */
public class LogzioParameterErrorException extends Exception {

    public LogzioParameterErrorException(String propertyName, String propertyValue) {
        super("Logzio Sender parameter "+propertyName+" value "+propertyValue+" is not valid.");
    }

    public LogzioParameterErrorException(String propertyName, String propertyValue, String explanition ) {
        super("Logzio Sender parameter "+propertyName+" value="+propertyValue+" is not valid: "+explanition);
    }
}
