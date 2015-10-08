package io.github.tcdl.msb.acceptance.bdd.util;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ListAppender extends AppenderSkeleton {

    private ConcurrentLinkedQueue<String> logEntries = new ConcurrentLinkedQueue<>();
    private static ListAppender instance;

    public ListAppender() {
        instance = this;
    }

    public static ListAppender getInstance() {
        Assert.assertNotNull("ListAppender has not yet been initialized. Did you include it in you log4j config?");
        return instance;
    }

    @Override
    protected void append(LoggingEvent event) {
        logEntries.add(event.getMessage().toString());
    }

    public void close() {
        logEntries.clear();
    }

    public boolean requiresLayout() {
        return false;
    }

    public void reset() {
        logEntries.clear();
    }

    public String findLine(String string, int retries, long pollIntervalMs) throws Exception {
        Optional<String> line;
        int numberOfRetries = retries;

        do {
            Thread.sleep(pollIntervalMs);
            line = logEntries.stream().filter(logEntry -> logEntry.contains(string)).distinct().findFirst();
        } while (!line.isPresent() && numberOfRetries-- > 0);

        return line.orElse(null);
    }
}