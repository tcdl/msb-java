package io.github.tcdl.msb.collector;

import io.github.tcdl.msb.RunOnShutdownScheduledExecutorDecorator;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link  TimeoutManager} class is responsible for scheduling tasks for execution and returning scheduled future for this tasks.
 */
public class TimeoutManager {

    private static final Logger LOG = LoggerFactory.getLogger(TimeoutManager.class);

    private RunOnShutdownScheduledExecutorDecorator timeoutExecutorDecorator;

    public TimeoutManager(int threadPoolSize) {
        timeoutExecutorDecorator = createTimeoutExecutorDecorator(threadPoolSize);
    }

    protected ScheduledFuture<?> enableResponseTimeout(int timeoutMs, Collector collector) {
        LOG.debug("Enabling response timeout for {} ms", timeoutMs);

        try {
            return timeoutExecutorDecorator.schedule(() -> {
                LOG.debug("Response timeout expired.");
                collector.end();
            }, timeoutMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            LOG.warn("Unable to schedule task for execution", e);
            return null;
        }

    }

    protected ScheduledFuture<?> enableAckTimeout(long timeoutMs, Collector collector) {
        LOG.debug("Enabling ack timeout for {} ms", timeoutMs);

        if (timeoutMs <= 0) {
            LOG.debug("Unable to schedule timeout with negative delay : {}", timeoutMs);
            return null;
        }

        try {
            return timeoutExecutorDecorator.schedule(() -> {
                if (collector.isAwaitingResponses()) {
                    LOG.debug("Ack timeout expired, but waiting for responses.");
                    return;
                }
                LOG.debug("Ack timeout expired.");
                collector.end();
            }, timeoutMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            LOG.warn("Unable to schedule task for execution", e);
            return null;
        }
    }

    private RunOnShutdownScheduledExecutorDecorator createTimeoutExecutorDecorator(int threadPoolSize) {
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("timer-provider-thread-%d")
                .build();

        return new RunOnShutdownScheduledExecutorDecorator("timeout manager", threadPoolSize, threadFactory);
    }

    public void shutdown() {
        LOG.info("Shutting down...");
        timeoutExecutorDecorator.shutdown();
        LOG.info("Shutdown complete");
    }
}