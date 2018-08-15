package io.logz.sender;

import io.logz.sender.exceptions.LogzioServerErrorException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.net.HttpURLConnection;

public class HttpsSyncSender {
    private final HttpsRequestConfiguration configuration;
    private final SenderStatusReporter reporter;


    public HttpsSyncSender(HttpsRequestConfiguration configuration, SenderStatusReporter reporter){
        this.configuration = configuration;
        this.reporter = reporter;
    }

    public HttpsRequestConfiguration getConfiguration() {
        return configuration;
    }

    private boolean shouldRetry(int statusCode) {
        boolean shouldRetry = true;
        switch (statusCode) {
            case HttpURLConnection.HTTP_OK:
            case HttpURLConnection.HTTP_BAD_REQUEST:
            case HttpURLConnection.HTTP_UNAUTHORIZED:
                shouldRetry = false;
                break;
        }
        return shouldRetry;
    }

    public void sendToLogzio(byte[] lineSeparatedPayload) throws LogzioServerErrorException {
        try {
            int currentRetrySleep = configuration.getInitialWaitBeforeRetryMS();

            for (int currTry = 1; currTry <= configuration.getMaxRetriesAttempts(); currTry++) {

                boolean shouldRetry = true;
                int responseCode = 0;
                String responseMessage = "";
                IOException savedException = null;

                try {
                    HttpURLConnection conn = (HttpURLConnection) configuration.getLogzioListenerUrl().openConnection();
                    conn.setRequestMethod(configuration.getRequestMethod());
                    conn.setRequestProperty("Content-length", String.valueOf(lineSeparatedPayload.length));
                    conn.setRequestProperty("Content-Type", "text/plain");
                    if (configuration.isCompressRequests()){
                        conn.setRequestProperty("Content-Encoding", "gzip");
                    }
                    conn.setReadTimeout(configuration.getSocketTimeout());
                    conn.setConnectTimeout(configuration.getConnectTimeout());
                    conn.setDoOutput(true);
                    conn.setDoInput(true);

                    conn.getOutputStream().write(lineSeparatedPayload);

                    responseCode = conn.getResponseCode();
                    responseMessage = conn.getResponseMessage();

                    if (responseCode == HttpURLConnection.HTTP_BAD_REQUEST) {
                        BufferedReader bufferedReader = null;
                        try {
                            StringBuilder problemDescription = new StringBuilder();
                            InputStream errorStream = conn.getErrorStream();
                            if (errorStream != null) {
                                bufferedReader = new BufferedReader(new InputStreamReader((errorStream)));
                                bufferedReader.lines().forEach(line -> problemDescription.append("\n").append(line));
                                reporter.warning(String.format("Got 400 from logzio, here is the output: %s", problemDescription));
                            }
                        } finally {
                            if (bufferedReader != null) {
                                try {
                                    bufferedReader.close();
                                } catch(Exception e) {}
                            }
                        }
                    }
                    if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
                        reporter.error("Logz.io: Got forbidden! Your token is not right. Unfortunately, dropping logs. Message: " + responseMessage);
                    }

                    shouldRetry = shouldRetry(responseCode);
                } catch (IOException e) {
                    savedException = e;
                    reporter.error("Got IO exception - " + e.getMessage());
                }

                if (!shouldRetry) {
                    reporter.info("Successfully sent bulk to logz.io, size: " + lineSeparatedPayload.length);
                    break;

                } else {

                    if (currTry == configuration.getMaxRetriesAttempts()){

                        if (savedException != null) {

                            reporter.error("Got IO exception on the last bulk try to logz.io", savedException);
                        }
                        // Giving up, something is broken on Logz.io side, we will try again later
                        throw new LogzioServerErrorException("Got HTTP " + responseCode + " code from logz.io, with message: " + responseMessage);
                    }

                    reporter.warning("Could not send log to logz.io, retry (" + currTry + "/" + configuration.getMaxRetriesAttempts()+ ")");
                    reporter.warning("Sleeping for " + currentRetrySleep + " ms and will try again.");
                    Thread.sleep(currentRetrySleep);
                    currentRetrySleep *= 2;
                }
            }

        } catch (InterruptedException e) {
            reporter.error("Got interrupted exception");
            Thread.currentThread().interrupt();
        }
    }

}
