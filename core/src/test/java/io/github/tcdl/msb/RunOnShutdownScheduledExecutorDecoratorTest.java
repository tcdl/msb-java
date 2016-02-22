package io.github.tcdl.msb;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RunOnShutdownScheduledExecutorDecoratorTest {

    // Methods that invoke shutdown may hang forever in case of some bug in shutdown. This value allows to prevent endless builds.
    private static final int SHUTDOWN_TIMEOUT = 20000;

    private static final int TIME_FAR_FUTURE = Integer.MAX_VALUE;
    private static final int TIME_IMMEDIATE = 0;

    private RunOnShutdownScheduledExecutorDecorator executorDecorator;

    @Before
    public void setUp() {
        executorDecorator = new RunOnShutdownScheduledExecutorDecorator("name", 1, new BasicThreadFactory.Builder().build());
    }

    @Test(timeout = SHUTDOWN_TIMEOUT)
    public void testShutdownWithOutstandingTask() {
        Runnable mockRunnable = mock(Runnable.class);
        executorDecorator.schedule(mockRunnable, TIME_FAR_FUTURE, TimeUnit.SECONDS);

        executorDecorator.shutdown();

        verify(mockRunnable, times(1)).run();
    }

    @Test(timeout = 20000)
    public void testShutdownWithCancelledTask() {
        Runnable mockRunnable = mock(Runnable.class);
        executorDecorator.schedule(mockRunnable, TIME_FAR_FUTURE, TimeUnit.SECONDS);
        Runnable mockCancelledRunnable = mock(Runnable.class);
        ScheduledFuture<?> scheduledFuture = executorDecorator.schedule(mockCancelledRunnable, TIME_FAR_FUTURE, TimeUnit.SECONDS);
        scheduledFuture.cancel(false);

        executorDecorator.shutdown();

        verify(mockRunnable, times(1)).run();
        verify(mockCancelledRunnable, never()).run();
    }

    @Test(timeout = 20000)
    public void testShutdownWithCompletedTask() throws Exception {
        Runnable mockCompletedRunnable = mock(Runnable.class);
        ScheduledFuture scheduleCompleted = executorDecorator.schedule(mockCompletedRunnable, TIME_IMMEDIATE, TimeUnit.SECONDS);
        verify(mockCompletedRunnable, timeout(1000).times(1)).run();

        Runnable mockRunnable = mock(Runnable.class);
        ScheduledFuture scheduleFuture =executorDecorator.schedule(mockRunnable, TIME_FAR_FUTURE, TimeUnit.SECONDS);

        assertFalse(scheduleCompleted.isCancelled());
        assertTrue(scheduleCompleted.isDone());

        assertFalse(scheduleFuture.isCancelled());
        assertFalse(scheduleFuture.isDone());

        assertNull(scheduleCompleted.get());

        executorDecorator.shutdown();

        verify(mockRunnable, times(1)).run();
        verify(mockCompletedRunnable, times(1)).run(); // verify the task is not invoked again

        assertTrue(scheduleFuture.isCancelled());
        assertTrue(scheduleFuture.isDone());

        try {
            scheduleFuture.get();
            fail("CancellationException is expected");
        } catch (CancellationException ex) {
            //ok
        }
    }

    @Test(timeout = 20000)
    public void testShutdownPreventsAnotherTaskSubmission() {
        Runnable subsequentTask = mock(Runnable.class);
        Runnable initialTask = Mockito.spy(new Runnable() {
            @Override
            public void run() {
                try {
                    executorDecorator.schedule(subsequentTask, TIME_FAR_FUTURE, TimeUnit.SECONDS);
                    Assert.fail("RejectedExecutionException is expected to be thrown");
                } catch (Exception e) {
                    Assert.assertTrue(e instanceof RejectedExecutionException);
                }
            }
        });
        executorDecorator.schedule(initialTask, TIME_FAR_FUTURE, TimeUnit.SECONDS);

        executorDecorator.shutdown();

        verify(initialTask, times(1)).run();
        verify(subsequentTask, never()).run();
    }
}