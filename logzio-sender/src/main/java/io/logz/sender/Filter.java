package io.logz.sender;

import com.google.gson.JsonObject;

public interface Filter {
    Filter parseFilterFromString(String strFilter);
    boolean filter(JsonObject log);
}
