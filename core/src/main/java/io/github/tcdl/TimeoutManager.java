package io.github.tcdl;

import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link  TimeoutManager} class is responsible for scheduling tasks for execution and returning scheduled future for this tasks.
 *
 * Created by ruslan
 */
public class TimeoutManager {

    private static final Logger LOG = LoggerFactory.getLogger(TimeoutManager.class);

    ScheduledExecutorService scheduledThreadPool;

    public TimeoutManager(int threadPoolSize) {
        scheduledThreadPool = createTimerThreadPool(threadPoolSize);
    }

    protected ScheduledFuture<?> enableResponseTimeout(int timeoutMs, Collector collector) {
        LOG.debug("Enabling response timeout for {} ms", timeoutMs);

        try {
            return scheduledThreadPool.schedule(() -> {
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
            return scheduledThreadPool.schedule(() -> {
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

    private ScheduledExecutorService createTimerThreadPool(int threadPoolSize) {
        LOG.info("Starting timer thread pool with {} threads ", threadPoolSize);
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("timer-provider-thread-%d").daemon(true)
                .build();
        return Executors.newScheduledThreadPool(threadPoolSize, threadFactory);
    }
}