package io.logz.sender.exceptions;

public class LogzioParameterErrorException extends Exception {

    public LogzioParameterErrorException(String property, String explanation ) {
        super(String.format("Problem with Logzio parameter(s): %s : %s", property, explanation));
    }

}
