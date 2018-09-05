package io.logz.sender;

public interface SenderStatusReporter {


    public void error(String msg);

    public void error(String msg, Throwable throwable);

    public void warning(String msg);

    public void warning(String msg, Throwable throwable);

    public void info(String msg);

    public void info(String msg, Throwable throwable);
}
