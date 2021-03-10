package io.logz.sender;

import com.google.gson.JsonObject;

public interface SimpleJQLFilterFactory {
    Filter simpleJQL = new Filter() {
        @Override
        public Filter parseFilterFromString(String strFilter) {
            return simpleJQL;
        }

        @Override
        public boolean filter(JsonObject log) {
            return true;
        }
    };
}
