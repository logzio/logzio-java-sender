[![Build Status](https://travis-ci.org/logzio/logzio-java-sender.svg?branch=master)](https://travis-ci.org/logzio/logzio-java-sender)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.logz.sender/logzio-sender/badge.svg)](http://mvnrepository.com/artifact/io.logz.sender/logzio-sender)

# Logzio java sender
This sender sends logs messages to your [Logz.io](http://logz.io) account in JSON format, using non-blocking threading, bulks, and HTTPS encryption. Please note that this sender requires java 8 and up.

### Technical Information
This appender uses [BigQueue](https://github.com/bulldog2011/bigqueue) implementation of persistent queue, so all logs are backed up to a local file system before being sent. Once you send a log, it will be enqueued in the queue and 100% non-blocking. There is a background task that will handle the log shipment for you. This jar is an "Uber-Jar" that shades both BigQueue, Gson and Guava to avoid "dependency hell".

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
| **drainTimeoutSec**       | *5*                                    | How often the sender should drain the queue (in seconds) |
| **logzioUrl**          | *https://listener.logz.io:8071*           | Logz.io URL, that can be found under "Log Shipping -> Libraries" in your account.
| **socketTimeout**       | *10 * 1000*                                    | The socket timeout during log shipment |
| **connectTimeout**       | *10 * 1000*                                    | The connection timeout during log shipment |
| **debug**       | *false*                                    | Print some debug messages to stdout to help to diagnose issues |
| **compressRequests**       | *false*                                    | Boolean. `true` if logs are compressed in gzip format before sending. `false` if logs are sent uncompressed. |

#### Parameters for in-memory queue
| Parameter          | Default                              | Explained  |
| ------------------ | ------------------------------------ | ----- |
| **inMemoryQueueCapacityInBytes**       | *1024 * 1024 * 100*                                | The amount of memory(bytes) we are allowed to use for the memory queue. If the value is -1 the sender will not limit the queue size.|
| **checkDiskSpaceInterval**       | *1000*                                | How often the should disk queue check for space (in milliseconds) |

#### Parameters for disk queue
| Parameter          | Default                              | Explained  |
| ------------------ | ------------------------------------ | ----- |
| **queueDir**          | *None*                                   | Where the sender should store the queue. It should be at least one folder in path.|
| **fileSystemFullPercentThreshold** | *98*                                   | The percent of used file system space at which the sender will stop queueing. When we will reach that percentage, the file system in which the queue is stored will drop all new logs until the percentage of used space drops below that threshold. Set to -1 to never stop processing new logs |
| **gcPersistedQueueFilesIntervalSeconds**       | *30*                                    | How often the disk queue should clean sent logs from disk |



### Code Example
From version 1.0.15 we use a builder to get Logz.io sender
```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonObject;

public class LogzioSenderExample {

    public static void main(String[] args) {
        
        HttpsRequestConfiguration conf = HttpsRequestConfiguration
                        .builder()
                        .setLogzioListenerUrl("https://listener.logz.io:8071")
                        .setLogzioType("javaSenderType")
                        .setLogzioToken("123456789")
                        .build();
        
        // Use one of the following implementations
        // 1) disk queue example 
        LogzioSender logzioSender = LogzioSender
                        .builder()
                        .setTasksExecutor(Executors.newScheduledThreadPool(3))
                        .setReporter(new LogzioStatusReporter(){/*implement simple interface for logging sender logging */})
                        .setHttpsRequestConfiguration(httpsRequestConfiguration)
                        .withDiskQueue()
                            .setQueueDir(queueDir)
                        .endDiskQueue()
                        .build();
        
        // 2) in memory queue example
        LogzioSender logzioSender = LogzioSender
                        .builder()
                        .setTasksExecutor(Executors.newScheduledThreadPool(3))
                        .setDrainTimeoutSec(drainTimeout)
                        .setReporter(new LogzioStatusReporter(){/*implement simple interface for logging sender logging */})
                        .setHttpsRequestConfiguration(conf)
                        .withInMemoryQueue()
                        .endInMemoryQueue()
                        .build();

        sender.start();
        JsonObject jsonMessage = createLogMessage(); // create JsonObject to send to logz.io
        sender.send(jsonMessage);
    }
}
```

Until version 1.0.14
```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonObject;

public class LogzioSenderExample {

    public static void main(String[] args) {
        LogzioSender sender =  LogzioSender.getOrCreateSenderByType(token, logzioType, drainTimeoutSec,fileSystemFullPercentThreshold, queueDir,
                        logzioUrl, socketTimeout, serverTimeout, true, new LogzioStatusReporter(){/*implement simple interface for logging sender logging */}, Executors.newScheduledThreadPool(2),30);
        sender.start();
        JsonObject jsonMessage = createLogMessage(); // create JsonObject to send to logz.io
        sender.send(jsonMessage);
    }
}
```



### Release notes
 - 1.0.16
   - add implementation for in memory queue
 - 1.0.12 
   - separating https request from the sender
   - add a builder for sender, http configuration, and queues implementation  
 - 1.0.11 fix shaded
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
