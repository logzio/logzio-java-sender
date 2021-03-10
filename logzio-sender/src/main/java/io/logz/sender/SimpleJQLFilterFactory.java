package io.logz.sender;

public class SimpleJQLFilterFactory implements Filter {
    @Override
    public Filter parseFilterFromString(String strFilter) {
        return this;
    }

    @Override
    public boolean filter(String log) {
        return true;
    }
}

