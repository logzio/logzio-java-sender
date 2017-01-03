[![Build Status](https://travis-ci.org/logzio/logzio-java-sender.svg?branch=master)](https://travis-ci.org/logzio/logzio-java-sender)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.logz.sender-sender/badge.svg)](http://mvnrepository.com/artifact/io.logz.sender/logzio-java-sender)

# Logzio java sender
This sender sends logs messages to your [Logz.io](http://logz.io) account in JSON format, using non-blocking threading, bulks, and HTTPS encryption. Please note that this sender requires java 8 and up.

### Technical Information
This appender uses [BigQueue](https://github.com/bulldog2011/bigqueue) implementation of persistent queue, so all logs are backed up to a local file system before being sent. Once you send a log, it will be enqueued in the buffer and 100% non-blocking. There is a background task that will handle the log shipment for you. This jar is an "Uber-Jar" that shades both BigQueue, Gson and Guava to avoid "dependency hell".

### Installation from maven
```xml
<dependency>
    <groupId>io.logz.sender</groupId>
    <artifactId>logzio-sender</artifactId>
    <version>1.0.0</version>
</dependency>
```


### Parameters
| Parameter          | Default                              | Explained  |
| ------------------ | ------------------------------------ | ----- |
| **token**              | *None*                                 | Your Logz.io token, which can be found under "settings" in your account.
| **logzioType**               | *java*                                 | The [log type](http://support.logz.io/support/solutions/articles/6000103063-what-is-type-) for that sender |
| **drainTimeoutSec**       | *5*                                    | How often the sender should drain the buffer (in seconds) |
| **fileSystemFullPercentThreshold** | *98*                                   | The percent of used file system space at which the sender will stop buffering. When we will reach that percentage, the file system in which the buffer rests will drop all new logs until the percentage of used space drops below that threshold. Set to -1 to never stop processing new logs |
| **bufferDir**          | *None*                                   | Where the sender should store the buffer. It should be at least one folder in path.|
| **logzioUrl**          | *https://listener.logz.io:8071*           | Logz.io URL, that can be found under "Log Shipping -> Libraries" in your account.
| **socketTimeout**       | *10 * 1000*                                    | The socket timeout during log shipment |
| **connectTimeout**       | *10 * 1000*                                    | The connection timeout during log shipment |
| **debug**       | *false*                                    | Print some debug messages to stdout to help to diagnose issues |


### Code Example
```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonObject;

public class LogzioLogbackExample {

    public static void main(String[] args) {
        LogzioSender sender =  LogzioSender.getOrCreateSenderByType(token, logzioType, drainTimeoutSec,fileSystemFullPercentThreshold, bufferDir,
                        logzioUrl, socketTimeout, serverTimeout, true, new LogzioStatusReporter(){/*implement simple interface for logging sender logging */}, Executors.newScheduledThreadPool(2),30);
        sender.start();
        JsonObject jsonMessage = createLogMessage(); // create JsonObject to sent to logz.io
        sender.sent(jsonMessage);
    }
}
```



### Release notes
 - 1.0.0 - 1.0.2
   - Initial releases
   

### Contribution
 - Fork
 - Code
 - ```mvn test```
 - Issue a PR :)
