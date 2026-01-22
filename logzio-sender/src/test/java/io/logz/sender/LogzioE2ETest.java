package io.logz.sender;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.logz.sender.exceptions.LogzioParameterErrorException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests that validate actual log shipping to Logz.io.
 * 
 * Required environment variables:
 * - LOGZIO_TOKEN: Shipping token for sending logs
 * - LOGZIO_API_KEY: API token for querying logs
 * - ENV_ID: Unique identifier for test isolation
 * - LOGZIO_API_URL: (optional) API URL, defaults to https://api.logz.io
 */
@EnabledIfEnvironmentVariable(named = "LOGZIO_TOKEN", matches = ".+")
public class LogzioE2ETest {
    
    private static final Logger logger = LoggerFactory.getLogger(LogzioE2ETest.class);
    private static final int INGESTION_WAIT_SECONDS = 180;
    private static final int QUERY_RETRY_INTERVAL_SECONDS = 30;
    private static final int MAX_QUERY_RETRIES = 8;
    
    private static String logzioToken;
    private static String logzioApiKey;
    private static String logzioApiUrl;
    private static String envId;
    
    @BeforeAll
    static void setUp() {
        logzioToken = System.getenv("LOGZIO_TOKEN");
        logzioApiKey = System.getenv("LOGZIO_API_KEY");
        logzioApiUrl = System.getenv("LOGZIO_API_URL");
        envId = System.getenv("ENV_ID");
        
        if (logzioApiUrl == null || logzioApiUrl.isEmpty()) {
            logzioApiUrl = "https://api.logz.io";
        }
        
        if (envId == null || envId.isEmpty()) {
            envId = "e2e-local-" + System.currentTimeMillis();
        }
        
        logger.info("E2E Test Configuration:");
        logger.info("  ENV_ID: {}", envId);
        logger.info("  API URL: {}", logzioApiUrl);
    }
    
    @Test
    void testLogShippingAndRetrieval() throws Exception {
        assertNotNull(logzioToken, "LOGZIO_TOKEN environment variable is required");
        assertNotNull(logzioApiKey, "LOGZIO_API_KEY environment variable is required");
        
        String testMessage = "E2E test log message - " + envId;
        sendTestLogs(testMessage);
        
        logger.info("Waiting {} seconds for log ingestion...", INGESTION_WAIT_SECONDS);
        TimeUnit.SECONDS.sleep(INGESTION_WAIT_SECONDS);
        
        JsonArray logs = queryLogsWithRetry();
        
        assertNotNull(logs, "Should receive logs from Logz.io API");
        assertTrue(logs.size() > 0, "Should have at least one log entry");
        
        JsonObject firstLog = logs.get(0).getAsJsonObject();
        assertTrue(firstLog.has("message"), "Log should have 'message' field");
        assertTrue(firstLog.has("@timestamp"), "Log should have '@timestamp' field");
        assertTrue(firstLog.has("type"), "Log should have 'type' field");
        assertTrue(firstLog.has("env_id"), "Log should have 'env_id' field");
        
        assertEquals(envId, firstLog.get("env_id").getAsString(), "env_id should match");
        assertTrue(firstLog.get("message").getAsString().contains("E2E test log message"), 
                   "message should contain test content");
        
        logger.info("✅ E2E test passed! Retrieved {} log(s) from Logz.io", logs.size());
    }
    
    private void sendTestLogs(String testMessage) throws LogzioParameterErrorException, IOException, InterruptedException {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        
        try {
            HttpsRequestConfiguration config = HttpsRequestConfiguration.builder()
                    .setLogzioToken(logzioToken)
                    .setLogzioType(envId)  // Use ENV_ID as type for isolation
                    .setLogzioListenerUrl("https://listener.logz.io:8071")
                    .setSocketTimeout(30000)
                    .setConnectTimeout(30000)
                    .build();
            
            LogzioSender sender = LogzioSender.builder()
                    .setHttpsRequestConfiguration(config)
                    .setDebug(true)
                    .setDrainTimeoutSec(5)
                    .setTasksExecutor(executor)
                    .setReporter(new LogzioTestStatusReporter(logger))
                    .withInMemoryQueue()
                    .setCapacityInBytes(100 * 1024 * 1024)
                    .endInMemoryQueue()
                    .build();
            
            sender.start();
            
            for (int i = 1; i <= 3; i++) {
                JsonObject log = new JsonObject();
                log.addProperty("message", testMessage + " - log #" + i);
                log.addProperty("@timestamp", Instant.now().toString());
                log.addProperty("env_id", envId);
                log.addProperty("type", envId);
                log.addProperty("level", "INFO");
                log.addProperty("logger", "LogzioE2ETest");
                log.addProperty("test_run", i);
                
                sender.send(log);
                logger.info("Sent log #{}", i);
            }
            
            // Wait for drain
            TimeUnit.SECONDS.sleep(10);
            sender.stop();
            
            logger.info("Successfully sent test logs to Logz.io");
            
        } finally {
            executor.shutdownNow();
        }
    }
    
    private JsonArray queryLogsWithRetry() throws Exception {
        String query = String.format("env_id:%s AND type:%s", envId, envId);
        
        for (int attempt = 1; attempt <= MAX_QUERY_RETRIES; attempt++) {
            logger.info("Query attempt {}/{}: {}", attempt, MAX_QUERY_RETRIES, query);
            
            try {
                JsonArray logs = queryLogzioApi(query);
                if (logs != null && logs.size() > 0) {
                    return logs;
                }
                logger.info("No logs found yet, waiting {} seconds before retry...", QUERY_RETRY_INTERVAL_SECONDS);
            } catch (Exception e) {
                logger.warn("Query failed: {}", e.getMessage());
            }
            
            if (attempt < MAX_QUERY_RETRIES) {
                TimeUnit.SECONDS.sleep(QUERY_RETRY_INTERVAL_SECONDS);
            }
        }
        
        fail("Failed to retrieve logs after " + MAX_QUERY_RETRIES + " attempts");
        return null;
    }
    
    private JsonArray queryLogzioApi(String query) throws IOException {
        URL url = new URL(logzioApiUrl + "/v1/search");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        
        try {
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("X-API-TOKEN", logzioApiKey);
            conn.setDoOutput(true);
            
            JsonObject request = new JsonObject();
            JsonObject queryObj = new JsonObject();
            JsonObject queryStringObj = new JsonObject();
            queryStringObj.addProperty("query", query);
            queryObj.add("query_string", queryStringObj);
            request.add("query", queryObj);
            request.addProperty("from", 0);
            request.addProperty("size", 100);
            
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = request.toString().getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }
            
            int responseCode = conn.getResponseCode();
            
            if (responseCode == 200) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                    }
                    
                    JsonObject jsonResponse = JsonParser.parseString(response.toString()).getAsJsonObject();
                    if (jsonResponse.has("hits") && jsonResponse.getAsJsonObject("hits").has("hits")) {
                        JsonArray hits = jsonResponse.getAsJsonObject("hits").getAsJsonArray("hits");
                        JsonArray results = new JsonArray();
                        for (int i = 0; i < hits.size(); i++) {
                            results.add(hits.get(i).getAsJsonObject().getAsJsonObject("_source"));
                        }
                        logger.info("Query returned {} results", results.size());
                        return results;
                    }
                }
            } else {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                    }
                    logger.error("API error ({}): {}", responseCode, response);
                }
            }
            
            return new JsonArray();
            
        } finally {
            conn.disconnect();
        }
    }
}

