package io.github.tcdl.msb;

import com.google.common.collect.Maps;
import io.github.tcdl.msb.support.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This decorator around {@link ScheduledThreadPoolExecutor} executes all pending tasks (not yet cancelled or completed) during shutdown.
 */
public class RunOnShutdownScheduledExecutorDecorator {

    private static final Logger LOG = LoggerFactory.getLogger(RunOnShutdownScheduledExecutorDecorator.class);

    private final ConcurrentMap<Future<?>, Runnable> tasks = Maps.newConcurrentMap();
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
    private String name;

    public RunOnShutdownScheduledExecutorDecorator(String name, int corePoolSize, ThreadFactory threadFactory) {
        LOG.info("[scheduled thread pool decorator '{}'] Starting with {} threads ", name, corePoolSize);
        this.name = name;
        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(corePoolSize, threadFactory);

        // Discards any task scheduled for future execution
        scheduledThreadPoolExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        CleaningRunnable decorated = new CleaningRunnable(command);
        ScheduledFuture<?> future = scheduledThreadPoolExecutor.schedule(decorated, delay, unit);
        decorated.setFuture(future);
        tasks.put(future, command);
        return new CleaningScheduledFuture<>(future);
    }

    /**
     * Executes all pending tasks (not yet cancelled or completed).
     */
    public synchronized void shutdown() {
        Utils.gracefulShutdown(scheduledThreadPoolExecutor, "timeout");

        /* After graceful pool shutdown:

            1. No new tasks can be submitted into it
            2. All active tasks complete their execution
            3. All tasks pending in queue are cancelled (if a user tries to cancel the tasks once more, nothing happens)

            So we we can assume that {@link #tasks} cannot be modified at this point
         */
        LOG.info("[scheduled thread pool decorator '{}'] Executing pending tasks...", name);
        tasks.values().forEach(java.lang.Runnable::run);
        tasks.clear();
        LOG.info("[scheduled thread pool decorator '{}'] Completed pending tasks execution.", name);
    }

    /**
     * A Runnable that removes its future when running.
     */
    private class CleaningRunnable implements Runnable {
        private final Runnable delegate;
        private Future<?> future;

        /**
         * Creates a new RunnableWithFuture.
         *
         * @param delegate the Runnable to delegate to
         */
        public CleaningRunnable(Runnable delegate) {
            this.delegate = delegate;
        }

        /**
         * Associates a Future with the runnable.
         */
        public void setFuture(Future<?> future) {
            this.future = future;
        }

        @Override
        public void run() {
            tasks.remove(future);
            delegate.run();
        }
    }

    /**
     * A ScheduledFuture that removes its future when canceling.
     * <p>
     * This allows us to differentiate between tasks canceled by the user and the underlying
     * executor. Tasks canceled by the user are removed from "tasks".
     *
     * @param <V> The result type returned by this Future
     */
    private class CleaningScheduledFuture<V> implements ScheduledFuture<V> {
        private final ScheduledFuture<V> delegate;

        /**
         * @param delegate the future to delegate to
         */
        public CleaningScheduledFuture(ScheduledFuture<V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return delegate.getDelay(unit);
        }

        @Override
        public int compareTo(Delayed o) {
            return delegate.compareTo(o);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean result = delegate.cancel(mayInterruptIfRunning);

            if (result) {
                // Tasks canceled by users are removed from "tasks"
                tasks.remove(delegate);
            }
            return result;
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return delegate.get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.get(timeout, unit);
        }
    }
}
