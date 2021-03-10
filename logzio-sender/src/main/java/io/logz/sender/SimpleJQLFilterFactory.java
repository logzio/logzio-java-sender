package io.logz.sender;

import com.google.gson.JsonObject;

public class SimpleJQLFilterFactory implements Filter {
    @Override
    public Filter parseFilterFromString(String strFilter) {
        return this;
    }

    @Override
    public boolean filter(JsonObject log) {
        return true;
    }
}

