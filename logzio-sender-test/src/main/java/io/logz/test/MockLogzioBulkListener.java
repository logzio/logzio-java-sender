package io.logz.test;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.eclipse.jetty.io.RuntimeIOException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

public class MockLogzioBulkListener implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(MockLogzioBulkListener.class);
    private static final String LISTENER_ADDRESS = "127.0.0.1";

    private Server server;
    private Queue<LogRequest> logRequests = new ConcurrentLinkedQueue<>();
    private final String host;
    private final int port;
    private int malformedLogs = 0;

    private boolean isServerTimeoutMode = false;
    private boolean raiseExceptionOnLog = false;
    private int timeoutMillis = 10000;

    public void setFailWithServerError(boolean raiseExceptionOnLog) {
        this.raiseExceptionOnLog = raiseExceptionOnLog;
    }
    public void setServerTimeoutMode(boolean serverTimeoutMode) {
        this.isServerTimeoutMode = serverTimeoutMode;
    }
    public void setTimeoutMillis(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public MockLogzioBulkListener() throws IOException {
        this.host = LISTENER_ADDRESS;
        this.port = findFreePort();
        server = new Server(new InetSocketAddress(host, port));
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException{
                logger.debug("got request with query string: {} ({})", request.getQueryString(), this);
                if (isServerTimeoutMode) {
                    try {
                        Thread.sleep(timeoutMillis);
                        baseRequest.setHandled(true);
                        return;
                    }
                    catch (InterruptedException e) {
                        // swallow
                    }
                }
                // Bulks are \n delimited, so handling each log separately
                try (Stream<String> logStream = getLogsStream(request)) {
                    logStream.forEach(line -> {
                        if (raiseExceptionOnLog) {
                            throw new RuntimeException();
                        }

                        String queryString = request.getQueryString();
                        LogRequest tmpRequest = new LogRequest(queryString, line);
                        logRequests.add(tmpRequest);
                        logger.debug("got log: {} ", line);
                    });
                    logger.debug("Total number of logRequests {} ({})", logRequests.size(), logRequests);
                } catch (IllegalArgumentException e) {
                    malformedLogs++;
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                } finally {
                    baseRequest.setHandled(true);
                }
            }
        });
        logger.info("Created a mock listener ("+this+")");
    }

    private Stream<String> getLogsStream(HttpServletRequest request) throws IOException {
        String contentEncoding = request.getHeader("Content-Encoding");
        if (contentEncoding != null && request.getHeader("Content-Encoding").equals("gzip")) {
            GZIPInputStream gzipInputStream = new GZIPInputStream(request.getInputStream());
            Reader decoder = new InputStreamReader(gzipInputStream, "UTF-8");
            BufferedReader br = new BufferedReader(decoder);
            return br.lines();
        } else {
            return request.getReader().lines();
        }
    }

    private int findFreePort() throws IOException {
         int attempts = 1;
         int port = -1;
         while (attempts <= 3) {
             int availablePort = -1;
             try {
                 ServerSocket serverSocket = new ServerSocket(0);
                 serverSocket.close();
                 availablePort = serverSocket.getLocalPort();
                 port = availablePort;
                 break;
             } catch (BindException e) {
                 if (attempts++ == 3) {
                     throw new RuntimeException("Failed to get a non busy port: "+availablePort, e);
                 } else {
                     logger.info("Failed to start mock listener on port {}", availablePort);
                 }
             }
         }
         return port;
     }

    public void start() throws Exception {
        logger.info("Starting MockLogzioBulkListener");
        server.start();
        int attempts = 1;
        while (!server.isRunning()) {
            logger.info("Server has not started yet");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
            attempts++;
            if (attempts > 10) {
                throw new RuntimeIOException("Failed to start after multiple attempts");
            }
        }
        logger.info("Started listening on {}:{} ({})", host, port, this);
    }

    public void stop() {
        logger.info("Stopping MockLogzioBulkListener");
        try {
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
        int attempts = 1;
        while (server.isRunning()) {
            logger.info("Server has not stopped yet");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
            attempts++;
            if (attempts > 10) {
                throw new RuntimeIOException("Failed to stop after multiple attempts");
            }
        }
        logger.info("Stopped listening on {}:{} ({})", host, port, this);
    }

    @Override
    public void close() throws IOException {
        stop();
    }

    public Collection<LogRequest> getReceivedMsgs() {
        return Collections.unmodifiableCollection(logRequests);
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    public static class LogRequest {
        private final String token;
        private final String type;
        private final JsonObject jsonObject;

        public LogRequest(String queryString, String logLine) {
            Map<String, String> paramToValueMap = Splitter.on('&').withKeyValueSeparator('=').split(queryString);
            if (paramToValueMap.containsKey("token")) {
                token = paramToValueMap.get("token");
            } else {
                throw new IllegalArgumentException("Token not found in query string: "+queryString);
            }

            if (paramToValueMap.containsKey("type")) {
                type = paramToValueMap.get("type");
            } else {
                throw new IllegalArgumentException("Token not found in query string: "+queryString);
            }

            try {
                jsonObject = new JsonParser().parse(logLine).getAsJsonObject();
            } catch (Exception e) {
                throw new IllegalArgumentException("Not a valid json received in body of request. logLine = "
                    + logLine, e);
            }
        }

        public String getToken() {
            return token;
        }

        public String getType() {
            return type;
        }

        public JsonObject getJsonObject() {
            return jsonObject;
        }

        public String getMessage() {
            return getStringFieldOrNull("message");
        }

        public String getLogger() {
            return getStringFieldOrNull("logger");
        }

        public String getLogLevel() {
            return getStringFieldOrNull("loglevel");
        }

        public String getHost() {
            return getStringFieldOrNull("hostname");
        }

        public String getStringFieldOrNull(String fieldName) {
            if (jsonObject.get(fieldName) == null) return null;
            return jsonObject.get(fieldName).getAsString();
        }

        @Override
        public String toString() {
            return "[Token = "+token +", type = "+type+"]: " + jsonObject.toString();
        }
    }

    public Optional<LogRequest> getLogByMessageField(String msg) {
        return logRequests.stream()
                .filter(r -> r.getMessage() != null && r.getMessage().equals(msg))
                .findFirst();
    }

    public int getNumberOfReceivedLogs() {
        return logRequests.size();
    }

    public int getNumberOfReceivedMalformedLogs() {
        return malformedLogs;
    }

    public MockLogzioBulkListener.LogRequest assertLogReceivedByMessage(String message) {
        Optional<MockLogzioBulkListener.LogRequest> logRequest = getLogByMessageField(message);
        assertThat(logRequest.isPresent()).describedAs("Log with message '"+message+"' received").isTrue();
        return logRequest.get();
    }

    public void assertNumberOfReceivedMsgs(int count) {
        assertThat(getNumberOfReceivedLogs())
                .describedAs("Messages on mock listener: {}", getReceivedMsgs())
                .isEqualTo(count);
    }

    public void assertNumberOfReceivedMalformedMsgs(int count) {
        assertThat(getNumberOfReceivedMalformedLogs())
                .describedAs("Malformed messages on mock listener: {}", malformedLogs)
                .isEqualTo(count);
    }

    public void assertLogReceivedIs(String message, String token, String type, String loggerName, String level) {
        MockLogzioBulkListener.LogRequest log = assertLogReceivedByMessage(message);
        assertLogReceivedIs(log, token, type, loggerName, level);
    }

    public void assertLogReceivedIs(MockLogzioBulkListener.LogRequest log, String token, String type, String loggerName, String level) {
        assertThat(log.getToken()).isEqualTo(token);
        assertThat(log.getType()).isEqualTo(type);
        assertThat(log.getLogger()).isEqualTo(loggerName);
        assertThat(log.getLogLevel()).isEqualTo(level);
    }


}