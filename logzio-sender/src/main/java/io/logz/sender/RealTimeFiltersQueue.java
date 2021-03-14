package io.logz.sender;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RealTimeFiltersQueue implements LogsQueue{

    private List<RTFilter> RTQueryRTFilters = new ArrayList<>();
    private List<RTFilter> defaultRTFilters;
    private LogsQueue filteredQueue;
    private SenderStatusReporter reporter;


    public RealTimeFiltersQueue(List<RTFilter> defaultRTFilters, LogsQueue filteredQueue, SenderStatusReporter reporter) {
        this.defaultRTFilters = defaultRTFilters;
        this.filteredQueue = filteredQueue;
        this.reporter = reporter;
    }

    private boolean shouldEnqueue(String log) {
        for (RTFilter RTQRTFilter : RTQueryRTFilters) {
            if (RTQRTFilter.filter(log)) {
                return true;
            }
        }

        for (RTFilter RTQRTFilter : defaultRTFilters) {
            if (RTQRTFilter.filter(log)) {
                return true;
            }
        }
        return false;
    }

    public void setRTQueryFilters(List<RTFilter> RTFilters) {
        this.RTQueryRTFilters = RTFilters;
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
        private List<RTFilter> defaultRTFilters = new ArrayList<>();
        private LogsQueue filteredQueue;
        private SenderStatusReporter reporter;


        public RealTimeFiltersQueue.Builder setDefaultFilters(List<RTFilter> defaultRTFilters) {
            this.defaultRTFilters = defaultRTFilters;
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
            return new RealTimeFiltersQueue(defaultRTFilters, filteredQueue, reporter);
        }
    }

//    public static RealTimeFiltersQueue.Builder builder(LogzioSender.Builder context){
//        return new RealTimeFiltersQueue.Builder();
//    }
}
