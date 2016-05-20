package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

/**
 * This {@link MessageHandlerInvoker} implementation gives an ability to execute {@link io.github.tcdl.msb.MessageHandler}
 * sequentially for messages with the same "groupId" (resolved by {@link MessageGroupStrategy} provided) while
 * messages with different "groupId" could be processed in parallel.
 */
public class GroupedExecutorBasedMessageHandlerInvoker extends ExecutorBasedMessageHandlerInvoker {

    private static final Logger LOG = LoggerFactory.getLogger(GroupedExecutorBasedMessageHandlerInvoker.class);

    private final ExecutorService[] executors;

    private final MessageGroupStrategy messageGroupStrategy;
    private final int numberOfThreads;

    public GroupedExecutorBasedMessageHandlerInvoker(int numberOfThreads, int queueCapacity,
            ConsumerExecutorFactory consumerExecutorFactory, MessageGroupStrategy messageGroupStrategy) {
        super(consumerExecutorFactory);
        this.messageGroupStrategy = messageGroupStrategy;
        this.numberOfThreads = numberOfThreads;

        executors = new ExecutorService[numberOfThreads];

        IntStream
                .range(0, numberOfThreads)
                .forEach(i -> executors[i] =
                                consumerExecutorFactory.createConsumerThreadPool(1, queueCapacity));
    }

    @Override
    protected void doSubmitTask(MessageProcessingTask task, Message message) {
        int executorKey = getExecutorKey(message);
        executors[executorKey].submit(task);
    }

    private int getExecutorKey(Message message) {
        Optional<Integer> messageGroupId = messageGroupStrategy.getMessageGroupId(message);
        if(messageGroupId.isPresent()) {
            return Math.abs(messageGroupId.get() % numberOfThreads);
        } else {
            return RandomUtils.nextInt(0, numberOfThreads);
        }
    }

    @Override
    public void shutdown() {
        Arrays
                .stream(executors)
                .forEach(executor -> Utils.gracefulShutdown(executor, "consumer"));
    }
}
