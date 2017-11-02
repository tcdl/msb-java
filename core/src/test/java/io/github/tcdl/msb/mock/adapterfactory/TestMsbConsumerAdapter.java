package io.github.tcdl.msb.mock.adapterfactory;

import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.adapters.ConsumerAdapter;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class TestMsbConsumerAdapter implements ConsumerAdapter {

    private final Set<RawMessageHandler> rawMessageHandlers = new HashSet<>();
    private final TestMsbStorageForAdapterFactory storage;

    private final String namespace;

    public TestMsbConsumerAdapter(String namespace, TestMsbStorageForAdapterFactory storage) {
        this.namespace = namespace;
        this.storage = storage;
    }

    @Override
    public void subscribe(RawMessageHandler onMessageHandler) {
        rawMessageHandlers.add(onMessageHandler);
    }

    @Override
    public void unsubscribe() {

    }

    @Override
    public Optional<Long> messageCount() {
        throw new UnsupportedOperationException("This method is not implemented in this test class.");
    }

    public void pushTestMessage(String jsonMessage) {
        AcknowledgementHandlerInternal ackHandler = mock(AcknowledgementHandlerInternal.class);
        rawMessageHandlers.forEach((handler)-> handler.onMessage(jsonMessage, ackHandler));
    }
}
