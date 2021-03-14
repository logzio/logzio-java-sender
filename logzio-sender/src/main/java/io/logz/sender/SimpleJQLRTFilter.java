package io.logz.sender;

public class SimpleJQLRTFilter implements RTFilter {

    @Override
    public boolean filter(String log) {
        return true;
    }
}

