package io.logz.sender;

public interface Filter {
    Filter parseFilterFromString(String strFilter);
    boolean filter(String log);
}
