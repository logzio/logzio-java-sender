package io.logz.sender;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RealTimeFiltersQueue implements LogsQueue{

    private Filter[] RTQueryFilters;
    private Filter[] defaultFilters;
    private LogsQueue filteredQueue;
    private SenderStatusReporter reporter;


    public RealTimeFiltersQueue(Filter[] realTimeQueryFilters, Filter[] defaultFilters, LogsQueue filteredQueue, SenderStatusReporter reporter) {
        this.RTQueryFilters = realTimeQueryFilters;
        this.defaultFilters = defaultFilters;
        this.filteredQueue = filteredQueue;
        this.reporter = reporter;
    }

    private boolean shouldEnqueue(JsonObject log) {
        for (Filter RTQFilter : RTQueryFilters) {
            if (RTQFilter.filter(log)) {
                return true;
            }
        }

        for (Filter RTQFilter : defaultFilters) {
            if (RTQFilter.filter(log)) {
                return false;
            }
        }
        return true;

    }

    @Override
    public void enqueue(byte[] log) {
        String strJson = new String(log, StandardCharsets.UTF_8);
        try {

            JsonObject jsonMsg = new Gson().fromJson(strJson, JsonObject.class);
            if (shouldEnqueue(jsonMsg)) {
                filteredQueue.enqueue(strJson.getBytes(StandardCharsets.UTF_8));
            }
        } catch (ClassCastException e) {
            reporter.error("failed to create msg object", e);
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

    public static class Builder {
        private Filter[] RTQueryFilters = new  Filter[0];
        private Filter[] defaultFilters = new  Filter[0];
        private LogsQueue filteredQueue;
        private SenderStatusReporter reporter;

        public RealTimeFiltersQueue.Builder setRealTimeQueryFilters(Filter[] realTimeQueryFilters) {
            this.RTQueryFilters = realTimeQueryFilters;
            return this;
        }

        public RealTimeFiltersQueue.Builder setDefaultFilters(Filter[] defaultFilters) {
            this.defaultFilters  = defaultFilters ;
            return this;
        }

        public RealTimeFiltersQueue.Builder setFilteredQueue(LogsQueue filteredQueue) {
                this.filteredQueue = filteredQueue;
                return this;
        }

        public RealTimeFiltersQueue.Builder setReporter(SenderStatusReporter reporter) {
            this.reporter = reporter;
            return this;
        }

        public RealTimeFiltersQueue build() {
            return new RealTimeFiltersQueue(RTQueryFilters, defaultFilters, filteredQueue, reporter);
        }
    }

//    public static RealTimeFiltersQueue.Builder builder(LogzioSender.Builder context){
//        return new RealTimeFiltersQueue.Builder();
//    }
}
