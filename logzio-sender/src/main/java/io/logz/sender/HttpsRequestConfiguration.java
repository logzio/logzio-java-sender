package io.logz.sender;

import io.logz.sender.exceptions.LogzioParameterErrorException;

import java.net.MalformedURLException;
import java.net.URL;

import static java.util.Objects.requireNonNull;

public class HttpsRequestConfiguration {
    private final int initialWaitBeforeRetryMS;
    private final int maxRetriesAttempts;
    private final int socketTimeout;
    private final int connectTimeout;
    private final String requestMethod;
    private final String logzioToken;
    private final String logzioType;
    private final URL logzioListenerUrl;

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

    private final boolean compressRequests;


    public HttpsRequestConfiguration(String logzioToken,
                                     int maxRetriesAttempts, int initialWaitBeforeRetryMS, int socketTimeout,
                                     int connectTimeout, String requestMethod, URL logzioListenerUrl,
                                     boolean compressRequests, String logzioType) throws LogzioParameterErrorException {
        this.maxRetriesAttempts = maxRetriesAttempts;
        this.initialWaitBeforeRetryMS = initialWaitBeforeRetryMS;
        this.socketTimeout = socketTimeout;
        this.connectTimeout = connectTimeout;
        this.requestMethod = requestMethod;
        this.logzioListenerUrl = logzioListenerUrl;
        this.logzioToken = logzioToken;
        this.compressRequests = compressRequests;
        this.logzioType = logzioType;
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
            if (logzioListenerUrl.equals(this.logzioListenerUrl)) {
                return this;
            }
            this.logzioListenerUrl = logzioListenerUrl;
            return this;
        }

        public Builder setCompressRequests(boolean compressRequests) {
            this.compressRequests = compressRequests;
            return this;
        }

        private URL createURL(String url) throws MalformedURLException {
                return logzioType == null ?
                        new URL(url + "/?token=" + logzioToken) :
                        new URL(url + "/?token=" + logzioToken + "&type=" + logzioType);
        }

        public HttpsRequestConfiguration build() throws LogzioParameterErrorException {
            URL url;
            try {
                url = createURL(logzioListenerUrl);
            } catch (MalformedURLException e){
                throw new LogzioParameterErrorException("logzioUrl="+logzioListenerUrl+" token="+logzioToken+" type="+logzioType, "For some reason could not initialize URL. Cant recover.." + e);
            }
            return new HttpsRequestConfiguration(
                    requireNonNull(logzioToken, "logzioToken can't be null"),
                    maxRetriesAttempts,
                    initialWaitBeforeRetryMS,
                    socketTimeout,
                    connectTimeout,
                    requestMethod,
                    url,
                    compressRequests,
                    logzioType);
        }
    }
}