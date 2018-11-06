package io.github.tcdl.msb.threading;

public class MessageHandlerInvokerFactoryImpl implements MessageHandlerInvokerFactory {

    private final ConsumerExecutorFactory consumerExecutorFactory;

    public MessageHandlerInvokerFactoryImpl(ConsumerExecutorFactory consumerExecutorFactory) {
        this.consumerExecutorFactory = consumerExecutorFactory;
    }

    @Override
    public MessageHandlerInvoker createDirectHandlerInvoker() {
        return new DirectMessageHandlerInvoker();
    }

    @Override
    public MessageHandlerInvoker createExecutorBasedHandlerInvoker(int numberOfThreads, int queueCapacity) {
        return new ThreadPoolMessageHandlerInvoker(numberOfThreads, queueCapacity, consumerExecutorFactory);
    }
}
