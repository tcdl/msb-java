package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.adapters.ConsumerAdapter;

import java.util.Optional;

public class ActiveMQConsumerAdapter implements ConsumerAdapter {
    @Override
    public void subscribe(RawMessageHandler onMessageHandler) {

    }

    @Override
    public void unsubscribe() {

    }

    @Override
    public Optional<Long> messageCount() {
        return Optional.empty();
    }

    @Override
    public Optional<Boolean> isConnected() {
        return Optional.empty();
    }
}
