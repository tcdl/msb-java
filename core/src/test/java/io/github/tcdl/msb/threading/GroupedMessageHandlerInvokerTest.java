package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GroupedMessageHandlerInvokerTest {
    private static final int CONFIG_THREADS = 5;
    private static final int CONFIG_QUEUE = -1;

    private ExecutorService[] executors;

    @Mock
    AcknowledgementHandlerInternal acknowledgeHandler;

    @Mock
    MessageHandler messageHandler;

    @Mock
    ConsumerExecutorFactory consumerExecutorFactory;

    @Mock
    MsbConfig msbConfig;

    @Mock
    MessageGroupStrategy messageGroupStrategy;

    private Message message = TestUtils.createMsbRequestMessage("any0", "any0");

    private Message message1 = TestUtils.createMsbRequestMessage("any1", "any1");

    private Message message2 = TestUtils.createMsbRequestMessage("any2", "any2");

    private GroupedMessageHandlerInvoker invoker;

    @Before
    public void setUp() throws Exception {

        executors = new ExecutorService[CONFIG_THREADS];

        for(int i = 0; i< CONFIG_THREADS ; i++) {
            executors[i] = mock(ExecutorService.class);
            when(executors[i].awaitTermination(10, TimeUnit.SECONDS)).thenReturn(true);
        }

        when(msbConfig.getConsumerThreadPoolSize()).thenReturn(CONFIG_THREADS);
        when(msbConfig.getConsumerThreadPoolQueueCapacity()).thenReturn(CONFIG_QUEUE);

        when(consumerExecutorFactory.createConsumerThreadPool(1, CONFIG_QUEUE))
                .thenReturn(executors[0], Arrays.copyOfRange(executors, 1, CONFIG_THREADS));

        List<MessageHandlerInvoker> invokers = IntStream
                .range(0, CONFIG_THREADS)
                .boxed()
                .map(i -> new ThreadPoolMessageHandlerInvoker(1, CONFIG_QUEUE, consumerExecutorFactory))
                .collect(Collectors.toList());
        invoker = new GroupedMessageHandlerInvoker<>(invokers, messageGroupStrategy);

        when(messageGroupStrategy.getMessageGroupId(message)).thenReturn(Optional.of(0));
        when(messageGroupStrategy.getMessageGroupId(message1)).thenReturn(Optional.of(1));
        when(messageGroupStrategy.getMessageGroupId(message2)).thenReturn(Optional.of(2));
    }

    @Test
    public void testExecutorsInitialized() {
        verify(consumerExecutorFactory, times(CONFIG_THREADS)).createConsumerThreadPool(1, CONFIG_QUEUE);
    }

    @Test
    public void testMessageHandling() {
        invoker.execute(messageHandler, message, acknowledgeHandler);
        verify(messageHandler, never()).handleMessage(any(), any());
        ArgumentCaptor<MessageProcessingTask> taskCaptor = ArgumentCaptor.forClass(MessageProcessingTask.class);
        verify(executors[0], times(1)).submit(taskCaptor.capture());
        MessageProcessingTask task = taskCaptor.getValue();

        assertEquals(message, task.getMessage());
        assertEquals(messageHandler, task.getMessageHandler());
        assertEquals(acknowledgeHandler, task.getAckHandler());
    }

    @Test
    public void testMessageRouting() {
        invoker.execute(messageHandler, message, acknowledgeHandler);
        verify(executors[0], times(1)).submit(any(MessageProcessingTask.class));

        verify(executors[1], times(0)).submit(any(MessageProcessingTask.class));

        invoker.execute(messageHandler, message2, acknowledgeHandler);
        verify(executors[2], times(1)).submit(any(MessageProcessingTask.class));
    }

    @Test
    public void testMessageRoutingGroupOverflow() {
        when(messageGroupStrategy.getMessageGroupId(message)).thenReturn(Optional.of(CONFIG_THREADS * 1231 + 3));

        invoker.execute(messageHandler, message, acknowledgeHandler);
        verify(executors[3], times(1)).submit(any(MessageProcessingTask.class));
    }

    public void testMessageRoutingGroupNegative() {
        when(messageGroupStrategy.getMessageGroupId(message)).thenReturn(Optional.of(-1));

        invoker.execute(messageHandler, message, acknowledgeHandler);
        verify(executors[CONFIG_THREADS - 1], times(1)).submit(any(MessageProcessingTask.class));
    }

    @Test
    public void testMessageRoutingGroupMissing() {
        when(messageGroupStrategy.getMessageGroupId(message)).thenReturn(Optional.empty());

        Set<ExecutorService> executorsInvolved = new HashSet<>();
        AtomicInteger invocationsCount = new AtomicInteger(0);

        Arrays.stream(executors).forEach((executor) ->
                when(executor.submit(any(MessageProcessingTask.class)))
                .thenAnswer((invocation) -> {
                    executorsInvolved.add(executor);
                    invocationsCount.incrementAndGet();
                    return null;
                }));

        int executeCount = 0;
        int maxIterations = 5000;
        do {
            executeCount ++;
            invoker.execute(messageHandler, message, acknowledgeHandler);
            if(executeCount > maxIterations) {
                fail(String.format("All available executors should be involved when message group is defined,"
                        + " failed to check this requirement in %d iterations", maxIterations));
            }
        } while (executorsInvolved.size() < CONFIG_THREADS);

        assertEquals(executeCount, invocationsCount.get());
        assertEquals(CONFIG_THREADS, executorsInvolved.size());
    }

    @Test
    public void testShutdown() {
        Arrays.stream(executors).forEach((executor)->verify(executor, times(0)).shutdown());
        invoker.shutdown();
        Arrays.stream(executors).forEach((executor)->verify(executor, times(1)).shutdown());
    }
}