package io.logz.sender;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RealTimeFiltersQueue implements LogsQueue{

    private List<Filter> RTQueryFilters = new ArrayList<>();
    private List<Filter> defaultFilters;
    private LogsQueue filteredQueue;
    private SenderStatusReporter reporter;


    public RealTimeFiltersQueue(List<Filter> defaultFilters, LogsQueue filteredQueue, SenderStatusReporter reporter) {
        this.defaultFilters = defaultFilters;
        this.filteredQueue = filteredQueue;
        this.reporter = reporter;
    }

    private boolean shouldEnqueue(String log) {
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

    public void setRTQueryFilters(List<Filter> filters) {
        this.RTQueryFilters = filters;
    }

    @Override
    public void enqueue(byte[] log) {
        String strJson = new String(log, StandardCharsets.UTF_8);
        try {
            if (shouldEnqueue(strJson)) {
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
        private List<Filter> defaultFilters = new ArrayList<>();
        private LogsQueue filteredQueue;
        private SenderStatusReporter reporter;


        public RealTimeFiltersQueue.Builder setDefaultFilters(List<Filter> defaultFilters) {
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
            return new RealTimeFiltersQueue(defaultFilters, filteredQueue, reporter);
        }
    }

//    public static RealTimeFiltersQueue.Builder builder(LogzioSender.Builder context){
//        return new RealTimeFiltersQueue.Builder();
//    }
}
