package io.github.tcdl.msb.acceptance.bdd.util;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.Assert;

public class ListAppender extends AppenderBase<ILoggingEvent> {

    private volatile ConcurrentLinkedQueue<String> logEntries = new ConcurrentLinkedQueue<>();
    private volatile static ListAppender instance = new ListAppender();

    public static ListAppender getInstance() {
        Assert.assertNotNull("ListAppender has not yet been initialized. Did you include it in you logback config?");
        return instance;
    }

    @Override
    protected void append(ILoggingEvent event) {
        instance.logEntries.add(event.toString());
    }

    public void reset() {
        instance.logEntries.clear();
    }

    public String findLine(String string, int retries, long pollIntervalMs) throws Exception {
        Optional<String> line;
        int numberOfRetries = retries;

        do {
            Thread.sleep(pollIntervalMs);
            line = instance.logEntries.stream().filter(logEntry -> logEntry.contains(string)).distinct().findFirst();
        } while (!line.isPresent() && numberOfRetries-- > 0);

        return line.orElse(null);
    }
}