package io.github.tcdl.msb.threading;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface MessageHandlerInvokerFactory {

    MessageHandlerInvoker createDirectHandlerInvoker();

    MessageHandlerInvoker createExecutorBasedHandlerInvoker(int numberOfThreads, int queueCapacity);

    default MessageHandlerInvoker createGroupedExecutorBasedHandlerInvoker(
            int numberOfThreads, int queueCapacity, MessageGroupStrategy messageGroupStrategy) {
        List<MessageHandlerInvoker> invokers = IntStream
                .range(0, numberOfThreads)
                .boxed()
                .map(i -> createExecutorBasedHandlerInvoker(1, queueCapacity))
                .collect(Collectors.toList());
        return new GroupedMessageHandlerInvoker<>(invokers, messageGroupStrategy);
    }
}
