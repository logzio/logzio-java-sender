package io.logz.sender;

import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Predicate;
import net.minidev.json.JSONArray;

public class JsonPathRTFilter implements RTFilter {
    String strFilter;

    public JsonPathRTFilter(String strFilter) {
        this.strFilter = strFilter;
    }

    @Override
    public boolean filter(String json) {
        try {
            JSONArray filtered = JsonPath.parse(json).read(this.strFilter, new Predicate[0]);
            return !filtered.isEmpty();
        } catch (InvalidJsonException | InvalidPathException ex) {
            return false;
        }
    }
}
