package io.logz.sender;

import com.google.gson.JsonObject;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

public class JsonPathFilter implements Filter {
    String strFilter;

    public JsonPathFilter(String strFilter) {
        this.strFilter = strFilter;
    }

    @Override
    public Filter parseFilterFromString(String strFilter) {
        return this;
    }

    @Override
    public boolean filter(String json) {
        try {
            String filtered = (JsonPath.parse("[" + json + "]").read(strFilter).toString());
            JsonPath.parse("[" + json + "]").read("$..[?(@.id)]");
            return !filtered.equals("[]");
        } catch (InvalidJsonException | InvalidPathException ex) {
            return false;
        }
    }
}
