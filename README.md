[![Build Status](https://travis-ci.org/logzio/logzio-java-sender.svg?branch=master)](https://travis-ci.org/logzio/logzio-java-sender)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.logz.sender/logzio-sender/badge.svg)](http://mvnrepository.com/artifact/io.logz.sender/logzio-sender)

# Logzio java sender
This sender sends logs messages to your [Logz.io](http://logz.io) account in JSON format, using non-blocking threading, bulks, and HTTPS encryption. Please note that this sender requires java 8 and up.

### Technical Information
This appender uses [BigQueue](https://github.com/bulldog2011/bigqueue) implementation of persistent queue, so all logs are backed up to a local file system before being sent. Once you send a log, it will be enqueued in the buffer and 100% non-blocking. There is a background task that will handle the log shipment for you. This jar is an "Uber-Jar" that shades both BigQueue, Gson and Guava to avoid "dependency hell".

### Installation from maven
```xml
<dependency>
    <groupId>io.logz.sender</groupId>
    <artifactId>logzio-sender</artifactId>
    <version>${logzio-sender-version}</version>
</dependency>
```


### Parameters
| Parameter          | Default                              | Explained  |
| ------------------ | ------------------------------------ | ----- |
| **token**              | *None*                                 | Your Logz.io token, which can be found under "settings" in your account.
| **logzioType**               | *java*                                 | The [log type](http://support.logz.io/support/solutions/articles/6000103063-what-is-type-) for that sender |
| **drainTimeoutSec**       | *5*                                    | How often the sender should drain the buffer (in seconds) |
| **fileSystemFullPercentThreshold** | *98*                                   | The percent of used file system space at which the sender will stop buffering. When we will reach that percentage, the file system in which the buffer is stored will drop all new logs until the percentage of used space drops below that threshold. Set to -1 to never stop processing new logs |
| **bufferDir**          | *None*                                   | Where the sender should store the buffer. It should be at least one folder in path.|
| **logzioUrl**          | *https://listener.logz.io:8071*           | Logz.io URL, that can be found under "Log Shipping -> Libraries" in your account.
| **socketTimeout**       | *10 * 1000*                                    | The socket timeout during log shipment |
| **connectTimeout**       | *10 * 1000*                                    | The connection timeout during log shipment |
| **debug**       | *false*                                    | Print some debug messages to stdout to help to diagnose issues |
| **compressRequests**       | *false*                                    | Boolean. `true` if logs are compressed in gzip format before sending. `false` if logs are sent uncompressed. |


### Code Example
```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonObject;

public class LogzioSenderExample {

    public static void main(String[] args) {
        LogzioSender sender =  LogzioSender.getOrCreateSenderByType(token, logzioType, drainTimeoutSec,fileSystemFullPercentThreshold, bufferDir,
                        logzioUrl, socketTimeout, serverTimeout, true, new LogzioStatusReporter(){/*implement simple interface for logging sender logging */}, Executors.newScheduledThreadPool(2),30);
        sender.start();
        JsonObject jsonMessage = createLogMessage(); // create JsonObject to send to logz.io
        sender.send(jsonMessage);
    }
}
```



### Release notes
 - 1.0.11 fix stop threads.
 - 1.0.10 add gzip compression 
 - 1.0.9 add auto deploy
 - 1.0.8 fix st issue: [Shaded library still listed as dependencies](https://github.com/logzio/logzio-java-sender/issues/13)
 - 1.0.6 -1.0.7
   - add error message about reason of 400(BAD REQUEST)
 - 1.0.5 
   - add runtime dependency on slf4j-api
   - fix NulPointerException
 - 1.0.1-1.0.4
   - Fix dependencies issue
 - 1.0.0
   - Initial release
   

### Contribution
 - Fork
 - Code
 - ```mvn test```
 - Issue a PR :)
