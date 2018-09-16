package io.logz.sender;

import io.logz.sender.exceptions.LogzioParameterErrorException;

import java.net.MalformedURLException;
import java.net.URL;

public class HttpsRequestConfiguration {
    private final int initialWaitBeforeRetryMS;
    private final int maxRetriesAttempts;
    private final int socketTimeout;
    private final int connectTimeout;
    private final String requestMethod;
    private final String logzioToken;
    private final String logzioType;
    private final URL logzioListenerUrl;
    private final boolean compressRequests;

    public int getInitialWaitBeforeRetryMS() {
        return initialWaitBeforeRetryMS;
    }

    public int getMaxRetriesAttempts() {
        return maxRetriesAttempts;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public String getRequestMethod() {
        return requestMethod;
    }

    public String getLogzioToken() {
        return logzioToken;
    }

    public String getLogzioType() {
        return logzioType;
    }

    public URL getLogzioListenerUrl() {
        return logzioListenerUrl;
    }

    public boolean isCompressRequests() {
        return compressRequests;
    }

    private HttpsRequestConfiguration(String logzioToken,
                                     int maxRetriesAttempts, int initialWaitBeforeRetryMS, int socketTimeout,
                                     int connectTimeout, String requestMethod, String logzioListenerUrl,
                                     boolean compressRequests, String logzioType) throws LogzioParameterErrorException {
        this.maxRetriesAttempts = maxRetriesAttempts;
        this.initialWaitBeforeRetryMS = initialWaitBeforeRetryMS;
        this.socketTimeout = socketTimeout;
        this.connectTimeout = connectTimeout;
        this.requestMethod = requestMethod;

        if (logzioToken == null || logzioToken.isEmpty()) {
            throw new LogzioParameterErrorException("logzioToken = " + logzioToken, "logzioToken can't be empty string or null ");
        }

        this.logzioToken = logzioToken;
        this.compressRequests = compressRequests;
        this.logzioType = logzioType;

        try {
            this.logzioListenerUrl = createURL(logzioListenerUrl);
        } catch (MalformedURLException e){
            throw new LogzioParameterErrorException("logzioUrl=" + logzioListenerUrl + " token=" + logzioToken
                    + " type=" + logzioType, "URL is malformed. Cant recover.." + e);
        }
    }

    private URL createURL(String url) throws MalformedURLException {
        if (url == null || url.isEmpty()) {
            throw new MalformedURLException("Empty or null URL");
        }
        return logzioType == null ?
                new URL(url + "/?token=" + logzioToken) :
                new URL(url + "/?token=" + logzioToken + "&type=" + logzioType);
    }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private int maxRetriesAttempts = 3;
        private int initialWaitBeforeRetryMS = 2000;
        private int socketTimeout = 10 * 1000;
        private int connectTimeout = 10 * 1000;
        private String requestMethod = "POST";
        private String logzioType;
        private String logzioListenerUrl = "https://listener.logz.io:8071";
        private String logzioToken;
        private boolean compressRequests = false;

        public Builder setLogzioToken(String logzioToken){
            this.logzioToken = logzioToken;
            return this;
        }
        public Builder setInitialWaitBeforeRetryMS(int initialWaitBeforeRetryMS) {
            this.initialWaitBeforeRetryMS = initialWaitBeforeRetryMS;
            return this;
        }

        public Builder setSocketTimeout(int socketTimeout) {
            this.socketTimeout = socketTimeout;
            return this;
        }

        public Builder setMaxRetriesAttempts(int maxRetriesAttempts) {
            this.maxRetriesAttempts = maxRetriesAttempts;
            return this;
        }

        public Builder setConnectTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder setRequestMethod(String requestMethod) {
            this.requestMethod = requestMethod;
            return this;
        }

        public Builder setLogzioType(String logzioType){
            this.logzioType = logzioType;
            return this;
        }

        public Builder setLogzioListenerUrl(String logzioListenerUrl){
            this.logzioListenerUrl = logzioListenerUrl;
            return this;
        }

        public Builder setCompressRequests(boolean compressRequests) {
            this.compressRequests = compressRequests;
            return this;
        }

        public HttpsRequestConfiguration build() throws LogzioParameterErrorException {
            return new HttpsRequestConfiguration(
                    logzioToken,
                    maxRetriesAttempts,
                    initialWaitBeforeRetryMS,
                    socketTimeout,
                    connectTimeout,
                    requestMethod,
                    logzioListenerUrl,
                    compressRequests,
                    logzioType);
        }
    }
}