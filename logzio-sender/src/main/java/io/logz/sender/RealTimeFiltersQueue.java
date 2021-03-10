package io.logz.sender;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RealTimeFiltersQueue implements LogsQueue{

    private Filter[] RTQueryFilters;
    private Filter[] defaultFilters;
    private InMemoryQueue filteredQueue;
    private final SenderStatusReporter reporter;

    public RealTimeFiltersQueue(InMemoryQueue filteredQueue, SenderStatusReporter reporter) {
       this.filteredQueue = filteredQueue;
       this.reporter = reporter;
    }

    private boolean shouldEnqueue(JsonObject log) {
        for (Filter RTQFilter : RTQueryFilters) {
            if (RTQFilter.filter(log)) {
                return true;
            }
        }

        for (Filter RTQFilter : RTQueryFilters) {
            if (RTQFilter.filter(log)) {
                return false;
            }
        }
        return true;

    }

    @Override
    public void enqueue(byte[] log) {
        String strJson = new String(log, StandardCharsets.UTF_8);
        JsonObject jsonMsg = new Gson().fromJson(strJson, JsonObject.class);
        if (shouldEnqueue(jsonMsg)) {
            filteredQueue.enqueue(strJson.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public byte[] dequeue() {
        return filteredQueue.dequeue();
    }

    @Override
    public boolean isEmpty() {
        return filteredQueue.isEmpty();
    }

    @Override
    public void close() throws IOException {
        filteredQueue.close();
    }
}
